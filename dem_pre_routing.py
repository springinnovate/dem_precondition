import logging
import sys
import os
import hashlib
import pathlib

from osgeo import gdal, ogr
import ecoshard.routing
import taskgraph

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s '
        '[%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


def create_directory_hash(subwatershed_id):
    hash_str = hashlib.sha256(str(subwatershed_id).encode('utf-8')).hexdigest()
    return os.path.join(hash_str[:2], hash_str[2:4])


def extract_dem_for_subwatershed(dem_path, gpkg_path, subwatershed_fid, output_raster_path):
    # Example placeholder function for extracting DEM by subwatershed geometry.
    # 1) Open GPKG, get geometry of subwatershed FID
    vector = ogr.Open(gpkg_path)
    layer = vector.GetLayer()
    subwatershed_feature = layer.GetFeature(subwatershed_fid)
    geom = subwatershed_feature.GetGeometryRef()

    # 2) Create in-memory mask or use gdal.Warp to clip
    # This is a very rough example for illustration.
    # Replace 'layerName' with your actual layer name or an appropriate SQL where clause.
    warp_options = gdal.WarpOptions(
        format='GTiff',
        cutlineDSName=gpkg_path,
        cutlineWhere=f'FID = {subwatershed_fid}',
        dstNodata=-9999,
        cropToCutline=True)
    gdal.Warp(
        output_raster_path,
        dem_path,
        options=warp_options)

    # 3) If you need a local projection, apply a projection transform here.
    # This snippet just returns the path where the DEM is stored.
    return output_raster_path

def fill_and_route_dem(dem_path, filled_dem_path):
    # Using the ecoshard.routing library to fill pits/hydrological routing.
    # Adjust to your actual function calls.
    ecoshard.routing.fill_pits(dem_path, filled_dem_path)
    # Additional routing steps can be placed here if needed.
    return filled_dem_path

def build_index_entry(index_dict, subwatershed_fid, final_dem_path):
    # Store the final path in the index using the FID as key.
    index_dict[subwatershed_fid] = final_dem_path

def main():
    # Configure some placeholders
    dem_path = '/path/to/large_dem.tif'
    gpkg_path = '/path/to/subwatersheds.gpkg'
    workspace_dir = '/path/to/workspace'
    n_cpus = -1  # use all CPUs, or set an integer

    os.makedirs(workspace_dir, exist_ok=True)
    task_graph_obj = taskgraph.TaskGraph(workspace_dir, n_cpus, 5.0)

    # Open the GPKG to iterate subwatersheds
    vector = ogr.Open(gpkg_path)
    layer = vector.GetLayer()
    index_dict = {}

    for feature in layer:
        subwatershed_fid = feature.GetFID()
        hash_subdir = create_directory_hash(subwatershed_fid)
        out_dir = os.path.join(workspace_dir, hash_subdir, str(subwatershed_fid))
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)

        extracted_dem_path = os.path.join(out_dir, 'extracted_dem.tif')
        filled_dem_path = os.path.join(out_dir, 'filled_dem.tif')

        task_extract = task_graph_obj.add_task(
            func=extract_dem_for_subwatershed,
            args=(dem_path, gpkg_path, subwatershed_fid, extracted_dem_path),
            target_path_list=[extracted_dem_path],
            task_name=f'extract_dem_for_{subwatershed_fid}')

        task_fill = task_graph_obj.add_task(
            func=fill_and_route_dem,
            args=(extracted_dem_path, filled_dem_path),
            dependent_task_list=[task_extract],
            target_path_list=[filled_dem_path],
            task_name=f'fill_and_route_{subwatershed_fid}')

        def _build_index_wrapper(fid, path, index=index_dict):
            build_index_entry(index, fid, path)

        task_graph_obj.add_task(
            func=_build_index_wrapper,
            args=(subwatershed_fid, filled_dem_path),
            dependent_task_list=[task_fill],
            task_name=f'build_index_{subwatershed_fid}')

    task_graph_obj.join()
    task_graph_obj.close()

    # Here is where you'd persist the index to disk if needed
    # for example:
    # with open(os.path.join(workspace_dir, 'index.json'), 'w') as index_file:
    #     json.dump(index_dict, index_file)

if __name__ == '__main__':
    main()
