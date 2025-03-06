import logging
import sys
import os
import hashlib
import pathlib

from osgeo import gdal, ogr
from ecoshard.geoprocessing import routing
import taskgraph

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s '
        '[%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)

MAX_PIXEL_FILL_COUNT = 10000  # TODO: dynamically set this in a way that makes sense


def create_directory_hash(subwatershed_id):
    hash_str = hashlib.sha256(str(subwatershed_id).encode('utf-8')).hexdigest()
    return os.path.join(hash_str[:2], hash_str[2:4])


def extract_dem_for_subwatershed(dem_path, gpkg_path, subwatershed_fid, output_raster_path):
    # Example placeholder function for extracting DEM by subwatershed geometry.
    # 1) Open GPKG, get geometry of subwatershed FID
    #vector = ogr.Open(gpkg_path)
    #layer = vector.GetLayer()
    #subwatershed_feature = layer.GetFeature(subwatershed_fid)
    #geom = subwatershed_feature.GetGeometryRef()

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


def process_subwatershed(task_graph, global_dem_path, gpkg_path, subwatershed_fid, workspace_dir):
    # Adjust file paths as needed

    extracted_dem_path = os.path.join(
        workspace_dir, f'extracted_dem_{subwatershed_fid}.tif')
    filled_dem_raster_path = os.path.join(
        workspace_dir, f'filled_dem_{subwatershed_fid}.tif')
    flow_dir_path = os.path.join(
        workspace_dir, f'flow_dir_mfd_{subwatershed_fid}.tif')

    extract_dem_task = task_graph.add_task(
        func=extract_dem_for_subwatershed,
        args=(global_dem_path, gpkg_path, subwatershed_fid, extracted_dem_path),
        target_path_list=[extracted_dem_path],
        task_name=f'extract_dem_subwatershed_{subwatershed_fid}')

    fill_pits_task = task_graph.add_task(
        func=routing.fill_pits,
        args=((extracted_dem_path, 1), filled_dem_raster_path),
        kwargs={
            'working_dir': workspace_dir,
            'max_pixel_fill_count': MAX_PIXEL_FILL_COUNT
        },
        dependent_task_list=[extract_dem_task],
        target_path_list=[filled_dem_raster_path],
        task_name=f'fill_pits_subwatershed_{subwatershed_fid}')

    _ = task_graph.add_task(
        func=routing.flow_dir_mfd,
        args=((filled_dem_raster_path, 1), flow_dir_path),
        kwargs={'working_dir': workspace_dir},
        dependent_task_list=[fill_pits_task],
        target_path_list=[flow_dir_path],
        task_name=f'flow_dir_subwatershed_{subwatershed_fid}')

    return flow_dir_path


def build_index_entry(index_dict, subwatershed_fid, final_dem_path):
    # Store the final path in the index using the FID as key.
    index_dict[subwatershed_fid] = final_dem_path


def get_fid_list(gpkg_path):
    vector = ogr.Open(gpkg_path)
    layer = vector.GetLayer()
    fid_list = [feature.GetFID() for feature in layer]
    layer = None
    vector = None
    return fid_list


def main():
    global_dem_path = './data/astgtm_compressed.tif'
    gpkg_path = './data/global_lev05.gpkg'
    workspace_dir = 'dem_pre_route_workspace'
    gpkg_basename = os.path.basename(os.path.splitext(gpkg_path)[0])
    n_cpus = -1
    os.makedirs(workspace_dir, exist_ok=True)
    task_graph = taskgraph.TaskGraph(workspace_dir, n_cpus, 5.0)

    fid_list = get_fid_list(gpkg_path)
    index_dict = {}

    for subwatershed_fid in fid_list:
        hash_subdir = create_directory_hash(subwatershed_fid)
        subwatershed_tag = f'{gpkg_basename}_{subwatershed_fid}'
        local_workspace_dir = os.path.join(
            workspace_dir, hash_subdir, subwatershed_tag)
        os.makedirs(local_workspace_dir, exist_ok=True)

        process_subwatershed(
            task_graph, global_dem_path, gpkg_path,
            subwatershed_fid, workspace_dir)
        break  # TODO: just one for debugging

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
