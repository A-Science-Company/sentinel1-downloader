import os
import time
import shutil
import json
import concurrent.futures
from datetime import datetime, timedelta
from osgeo import ogr, osr
import pystac_client
import planetary_computer as pc
from collections import defaultdict
import requests
import shapely.geometry

# --- Helper Functions ---
def get_aoi_geometry_and_bbox(vector_file_path):
    """Get AOI geometry and bounding box in EPSG:4326"""
    # Open the vector file
    ds = ogr.Open(vector_file_path)
    if ds is None:
        raise ValueError(f"Could not open {vector_file_path}")
    
    # Get the layer and its spatial reference
    layer = ds.GetLayer()
    source_srs = layer.GetSpatialRef()
    
    # Create target spatial reference (EPSG:4326)
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(4326)
    
    # Create coordinate transformation if needed
    transform = None
    if source_srs and not source_srs.IsSame(target_srs):
        transform = osr.CoordinateTransformation(source_srs, target_srs)
    
    # Create a multi-polygon to store the union of all geometries
    union_geom = ogr.Geometry(ogr.wkbMultiPolygon)
    
    # Process each feature
    for feature in layer:
        geom = feature.GetGeometryRef().Clone()
        if geom is not None:
            # Flatten 3D to 2D if needed
            if geom.GetDimension() == 3:
                geom.FlattenTo2D()
            
            # Transform to EPSG:4326 if needed
            if transform:
                geom.Transform(transform)
            
            # Add to union
            union_geom = union_geom.Union(geom)
    
    # Get bounding box in EPSG:4326
    minx, maxx, miny, maxy = union_geom.GetEnvelope()
    bbox = [minx, miny, maxx, maxy]
    
    # Convert to Shapely geometry
    geojson_str = union_geom.ExportToJson()
    aoi_geometry = shapely.geometry.shape(json.loads(geojson_str))
    
    # Clean up
    union_geom = None
    ds = None
    
    return aoi_geometry, bbox

def download_tile(item, band, output_dir):
    """Download full tile without processing"""
    try:
        asset = item.assets[band]
        signed_url = pc.sign(asset.href)
        filename = f"{item.id}_{band}.tif".replace("/", "_")
        output_path = os.path.join(output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(output_path):
            print(f"File exists, skipping: {filename}")
            return output_path
        
        print(f"Downloading {filename}")
        
        # Download using requests with streaming
        with requests.get(signed_url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192 * 8):
                    f.write(chunk)
        
        # Verify file size
        if os.path.getsize(output_path) > 1024:
            print(f"Downloaded {filename} ({os.path.getsize(output_path)//1024} KB)")
            return output_path
        else:
            os.remove(output_path)
            print(f"File too small, deleted: {filename}")
            return None
    except Exception as e:
        print(f"Error downloading {item.id} ({band}): {e}")
        return None

def item_intersects_aoi(item, aoi_geometry):
    """Check if an item intersects with the AOI geometry"""
    try:
        # Get item geometry as Shapely object
        item_geom = shapely.geometry.shape(item.geometry)
        return aoi_geometry.intersects(item_geom)
    except Exception as e:
        print(f"Error checking intersection for {item.id}: {e}")
        return False

# --- Main Processing Function ---
def download_sentinel1_tiles(shapefile_path, start_date, end_date, base_folder, state_name):
    # Load AOI geometry and bbox
    try:
        aoi_geometry, bbox = get_aoi_geometry_and_bbox(shapefile_path)
        print(f"Loaded AOI geometry with bbox (EPSG:4326): {bbox}")
    except Exception as e:
        print(f"Error loading AOI: {e}")
        return

    # Setup directories
    os.makedirs(base_folder, exist_ok=True)
    
    # Connect to Planetary Computer
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=pc.sign_inplace,
    )

    # Search using bbox instead of geometry
    all_items = []
    date_ranges = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Create date chunks for searching
    chunk_size_days = 24  # Days per chunk
    while current_date <= end_dt:
        chunk_end = min(current_date + timedelta(days=chunk_size_days), end_dt)
        date_range = f"{current_date.strftime('%Y-%m-%d')}/{chunk_end.strftime('%Y-%m-%d')}"
        date_ranges.append(date_range)
        current_date = chunk_end + timedelta(days=1)
    
    # Search in chunks
    for date_range in date_ranges:
        print(f"Searching STAC catalog for {date_range}...")
        
        search_params = {
            "collections": ["sentinel-1-grd"],
            "bbox": bbox,
            "datetime": date_range,
            "query": {
                "sat:orbit_state": {"eq": "descending"},
                "sar:instrument_mode": {"eq": "IW"}
            },
            "limit": 100
        }
        
        try:
            search = catalog.search(**search_params)
            items = search.item_collection()
            print(f"Found {len(items)} items for {date_range}")
            all_items.extend(items)
        except Exception as e:
            print(f"STAC search failed for {date_range}: {e}")
            # Try with smaller chunk if failed
            if chunk_size_days > 7:
                print("Reducing chunk size and retrying...")
                chunk_size_days = max(7, chunk_size_days // 2)
                current_date = datetime.strptime(start_date, "%Y-%m-%d")
                date_ranges = []
                while current_date <= end_dt:
                    chunk_end = min(current_date + timedelta(days=chunk_size_days), end_dt)
                    date_range = f"{current_date.strftime('%Y-%m-%d')}/{chunk_end.strftime('%Y-%m-%d')}"
                    date_ranges.append(date_range)
                    current_date = chunk_end + timedelta(days=1)
                continue
    
    if not all_items:
        print("No items found. Exiting.")
        return
    
    print(f"Total items found in bbox: {len(all_items)}")
    
    # Filter items by actual geometry intersection
    filtered_items = []
    for item in all_items:
        if item_intersects_aoi(item, aoi_geometry):
            filtered_items.append(item)
    
    print(f"Items intersecting with AOI: {len(filtered_items)}")
    
    # Exit if no items found after filtering
    if not filtered_items:
        print("No intersecting items found. Exiting.")
        return

    # Group items by acquisition date
    items_by_date = defaultdict(list)
    for item in filtered_items:
        date_str = datetime.strptime(item.properties["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
        items_by_date[date_str].append(item)
    
    # Create 12-day processing cycles
    cycle_dates = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end_dt:
        cycle_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=12)
    
    # Process each cycle
    for cycle_start in cycle_dates:
        cycle_start_dt = datetime.strptime(cycle_start, "%Y-%m-%d")
        cycle_end_dt = cycle_start_dt + timedelta(days=11)
        print(f"\nProcessing cycle: {cycle_start} to {cycle_end_dt.strftime('%Y-%m-%d')}")
        
        # Collect items for this cycle
        cycle_items = []
        for date, items_list in items_by_date.items():
            date_dt = datetime.strptime(date, "%Y-%m-%d")
            if cycle_start_dt <= date_dt <= cycle_end_dt:
                cycle_items.extend(items_list)
        
        if not cycle_items:
            print("No items in this cycle. Skipping.")
            continue
            
        cycle_folder = os.path.join(base_folder, f"{cycle_start}_cycle")
        os.makedirs(cycle_folder, exist_ok=True)
        
        # Create band directories
        vv_dir = os.path.join(cycle_folder, "vv")
        vh_dir = os.path.join(cycle_folder, "vh")
        os.makedirs(vv_dir, exist_ok=True)
        os.makedirs(vh_dir, exist_ok=True)
        
        # Download all tiles in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            
            for item in cycle_items:
                for band in ["vv", "vh"]:
                    if band in item.assets:
                        output_dir = vv_dir if band == "vv" else vh_dir
                        futures.append(executor.submit(download_tile, item, band, output_dir))
            
            # Track results
            success_count = 0
            failure_count = 0
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception as e:
                    failure_count += 1
                    print(f"Error in download: {e}")
            
            print(f"Download summary: {success_count} successful, {failure_count} failed")
    
    print("\nDownload completed successfully!")

if __name__ == "__main__":
    # ===== USER CONFIGURATION =====
    SHAPEFILE_PATH = "shape_files/ap_districts.shp"
    STATE_NAME = "ap"
    SOURCE = "s1_grd"
    START_DATE = "2021-01-01"
    END_DATE = "2021-01-24"  # Smaller range for testing
    BASE_FOLDER = f"data/{STATE_NAME}/{SOURCE}"
    # ==============================
    
    download_sentinel1_tiles(
        shapefile_path=SHAPEFILE_PATH,
        start_date=START_DATE,
        end_date=END_DATE,
        base_folder=BASE_FOLDER,
        state_name=STATE_NAME
    )