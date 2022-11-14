from dagster import sensor,job,repository,DefaultSensorStatus,RunRequest
from ops import read_stream_data,trade_featurize_data,du_featurize_data,post_data_to_clh,del_consumed_files
import os


@job()
def update_clh():
    du_data,trade_data,consumed_files = read_stream_data()
    trade_insert_data = trade_featurize_data(trade_data)
    du_insert_data = du_featurize_data(du_data)
    status_codes = post_data_to_clh(du_insert_data,trade_insert_data)
    del_consumed_files(consumed_files,status_codes)


@sensor(job=update_clh,
        minimum_interval_seconds=5,
        default_status=DefaultSensorStatus.RUNNING)
def sensor_5_sec():
    if len(os.listdir('stream_data'))>1:
        yield RunRequest(run_key=None, run_config={})



@repository()
def my_repa():
    sensor_list = [sensor_5_sec]
    job_list = [update_clh]
    return sensor_list + job_list