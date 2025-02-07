# Global Preconditioned DEM Workflow

## Overview
This document outlines the process for creating a global preconditioned Digital Elevation Model (DEM) using ALOS data from Google Earth Engine (GEE). The workflow includes:
1. Extracting the DEM from GEE
2. Downloading the DEM tiles
3. Stitching the DEM into a global dataset
4. Parallelized pit-filling using HydroBASINS subwatersheds
5. Multi-Flow Direction (MFD) routing on the processed DEM

## Steps

### 1. Extract ALOS DEM from Google Earth Engine
A script is needed to retrieve ALOS DEM data from GEE. The script should:
- Access ALOS DEM from GEE.
- Define the global extent of the DEM.
- Export the DEM in manageable tiles.

### 2. Download DEM Tiles
Once exported, a process is required to:
- Copy the tiles from GEE to local storage or cloud storage.
- Maintain metadata for merging and processing.

### 3. Stitch the DEM into a Global Dataset
To create a seamless global DEM:
- Merge the downloaded tiles.
- Ensure proper alignment and resolution consistency.
- Handle any missing data or artifacts.

### 4. Parallelized Pit-Filling using HydroBASINS Subwatersheds
Pit-filling is required to remove sinks and depressions. This can be parallelized using HydroBASINS subwatersheds:
- Divide the global DEM into subwatershed regions.
- Assign each region to a processing unit.
- Apply pit-filling algorithms (e.g., Wang & Liu or FillDepressions).
- Merge the processed outputs while ensuring hydrological consistency.

### 5. Multi-Flow Direction (MFD) Routing
Once pit-filled, the DEM should be hydrologically conditioned using MFD:
- Compute flow directions using MFD.
- Generate flow accumulation layers.
- Verify consistency across boundaries.

## Next Steps
- Implement the GEE script to extract ALOS DEM.
- Develop a download automation script.
- Identify a tool or library to merge tiles.
- Determine best practices for parallel pit-filling.
- Set up MFD routing and validation.
