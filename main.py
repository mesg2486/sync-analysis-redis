import redis
import subprocess
import time
import logging
import base
import csv
import re
import csv
import requests
import os
import shutil
import helpers
import rppg  # model
import dialogue  # model
from emotion import go_emotion
from speaker import diarization
from decimal import Decimal

if __name__ == '__main__':
    NO_MESSAGE_COUNT = 0 
    Z_POP_KEY = 'SingleModelPreProcess'
    Z_INFO_SEP = ':'
    cache_dir = "./data"
    raw_input_dir = "./raw"
    asr_path = "./data/transcription.txt"
    diarization_path = "./data/diarization.txt"
    
    while True:
        local_cached_video_path=""
        start_time=time.time()
        
        resource_info = base.redis_instance.zpopmin(Z_POP_KEY)
        # [(b'meeting146:test/video/gitlab_video_2.mp4:0.6', 1709577847.4095476)]

        if len(resource_info) < 1:
            IS_LOCKED = 0
            logging.info('empty redis')
            time.sleep(20);
        else:
            logging.info("redis entry found...")
            # Reset cache
            logging.info("Resetting cache...")
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
            if os.path.exists(raw_input_dir):
                shutil.rmtree(raw_input_dir)
            
            # parse s3 path
            resource_info = str(resource_info[0][0])[2:-1]
            resource_info = base.remove_quotes(resource_info)
            resource_infos = resource_info.split(Z_INFO_SEP)
            meeting_id = resource_infos[0]
            s3_file_path = resource_infos[1]
            s3_file_name = s3_file_path.split("/")[2]
            logging.info(f"video detected {resource_info} {s3_file_name} {meeting_id}")

            # Download from s3
            local_cached_video_path, local_cached_filename = base.download_s3_resource(s3_file_path, raw_input_dir)

            # Resampling: Change FPS
            logging.info(f"Resampling video: {local_cached_filename}")
            resample_path = f"./data/{local_cached_filename}"
            output_data_path, duration = helpers.change_fps(local_cached_video_path, resample_path, 25)
            logging.info(f"Resampled. Video Duration is {duration}")
                            
            # Run rppg model
            logging.info(f"Running rppg model: {local_cached_filename}")
            rppg.go_rppg(local_cached_video_path, cache_dir)
            
            # count speakers
            logging.info(f"Counting speakers in the video...")
            speaker_count, jpg_files = helpers.get_speaker_count('data')
            logging.info(f"Speaker count: {speaker_count}")
            
            # upload rppg files
            
            # heatmap 
            helpers.process_heatmap(meeting_id)
            
            # extract audio
            wav_file = helpers.extract_audio(resample_path)
            
            # Run NLP model
            logging.info(f"Generating transcript...")
            transcription_segments = helpers.asr_transcribe(wav_file)
            helpers.save_transcription_to_file(transcription_segments, asr_path)

            # Run diarization model
            logging.info(f"Running diarization model...")
            speaker_chunks = diarization(wav_file, int(speaker_count))
            result_path = helpers.match_speakers_asr(speaker_chunks, meeting_id)
            
            # upload nlp files
            
            # Run emotion model 
            logging.info(f"Running emotion model...")
            diarization_result, emotion_data = helpers.process_diarization_and_emotion(diarization_path)
            emotion_labels = go_emotion(emotion_data)
            emotion_path = helpers.save_emotion_results(emotion_labels, diarization_result, meeting_id)
            
            # Dialogue model
            logging.info(f"Running dialogue model...")
            emotion_texts, text = helpers.load_emotion_data()
            dialogue_act_labels = dialogue.go_dialogue_act(text)
            unique_speakers = helpers.save_dialogue_act_labels(dialogue_act_labels, emotion_texts, meeting_id)
            # helpers.upload_participation_emotion('./data/a_results.csv', './data/v_results.csv', unique_speakers, meeting_id)
            base.upload_selected_files_to_endpoint(meeting_id, './data')

            # if process.returncode == 0:
            #     logging.info(f"processed in {process_finish_time - process_start_time}s")
            #     local_csv_out_file_path = 'exports/active_speakers.csv'
            #     base.upload_file_to_endpoint(meeting_id, local_csv_out_file_path)
            #     base.upload_timestamps(meeting_id, process_start_time, process_finish_time)
