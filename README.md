# Sentinel-1 GRD Download Pipeline

Python scripts for downloading Sentinel-1 Ground Range Detected (GRD) data from Microsoft Planetary Computer, optimized for large-scale agricultural monitoring.

## Key Features
- Downloads Sentinel-1 GRD data in VV and VH polarizations
- Organizes data into 12-day cycles matching Sentinel-1 revisit period
- Supports both bounding box and polygon-based AOI queries
- Parallel downloading for efficient data acquisition
- Automatically skips existing files for incremental downloads

## Scripts

### 1. `main.py` - Bounding Box Approach
- Uses the **4 corners** of the input geometry
- Downloads all tiles that intersect with the bounding box
- Faster processing but may download extra tiles outside actual AOI
- Ideal for rectangular regions or when precise boundaries aren't critical


### 2. `geom.py` - Bounding Box Approach
- Uses the exact polygon shape for intersection checks
- Downloads only tiles that intersect with the actual AOI polygon
- More precise but computationally intensive
- Ideal for irregular shapes or when minimizing data download is critical
