from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from .models import read_stacov, generate_geojson, generate_CSV_geojson,generate_MYCS2_geojson,generate_OPUSNET_geojson
import os
import json
from datetime import datetime
import pandas as pd
import boto3
from decouple import config
from .db_helper import fetch_all_opusnet_data

# Set up the S3 connection
s3 = boto3.resource(
    service_name='s3',
    region_name=config('AWS_DEFAULT_REGION'),
    aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY')
)

class StacovJsonView(APIView):
    def post(self, request):
        try:
            # Extract the date input from the frontend
            input_date_str = request.data.get('input', '')
            if not input_date_str:
                return Response({"error": "No date input provided"}, status=status.HTTP_400_BAD_REQUEST)
            if input_date_str['options'] == 'Static JSON + STACOV File' or input_date_str['options'] == 'Initial Load':

                # Convert the input date string to a datetime object
                input_date = datetime.strptime(input_date_str['date'], '%Y-%m-%dT%H:%M:%S.%fZ')

                # Format the date part for the filename
                day = input_date.strftime('%d')
                month = input_date.strftime('%b').lower()
                year = input_date.strftime('%y')

                # Construct the correct date part (e.g., '24apr16')
                date_part = f"{year}{month}{day}"
                
                # Construct the full file name
                file_name = f"{date_part}NOAM4.0_ambres_nfx20.stacov"
                
                
                # Create the full file path
                file_path = os.path.join(settings.BASE_DIR, 'static', file_name)
                
                if not os.path.exists(file_path):
                    
                    return Response({"error": "Data not found"}, status=status.HTTP_400_BAD_REQUEST)
                    
                
                # Open and process the STACOV file
                with open(file_path, 'rb') as file:
                    cdate, nsta, df_xyz = read_stacov(file)
                    geojson_str = generate_geojson(df_xyz)
                    geojson_data = json.loads(geojson_str)

                # Return the processed GeoJSON data
                return Response(geojson_data, status=status.HTTP_200_OK)
            elif input_date_str['options'] ==  'Over All Site Info':
                file_name = 'site_id.csv'
                file_path = os.path.join(settings.BASE_DIR, 'static', file_name)
                with open(file_path, 'rb') as file:
                    df = pd.read_csv(file)
                    geojson_csv_str = generate_CSV_geojson(df)
                    geojson_csv_data = json.loads(geojson_csv_str)
                return Response(geojson_csv_data,status=status.HTTP_200_OK)
            elif input_date_str['options'] == 'Over All Vs MYCS2':
                file_name_1 = 'site_id.csv'
                file_path_1 = os.path.join(settings.BASE_DIR, 'static', file_name_1)
                with open(file_path_1, 'rb') as file:
                    df_1 = pd.read_csv(file)
                input_date = datetime.strptime(input_date_str['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
                file_name = 'mycs2_predictions.csv'
                # Fetch the MYCS2 predictions CSV from S3
                obj = s3.Bucket('cors-dashboard-dataset').Object(file_name).get()
                df = pd.read_csv(obj['Body'])
                geojson_MYCS2_str = generate_MYCS2_geojson(df,input_date,df_1)
                geojson_MYCS2_data = json.loads(geojson_MYCS2_str)
                return Response(geojson_MYCS2_data,status=status.HTTP_200_OK)
            elif input_date_str['options'] == 'OPUSNET Data':
                # Convert the input date string to a datetime object
                input_date = datetime.strptime(input_date_str['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
                file_name = 'opusnet_converted_corrected.csv'
                file_path = os.path.join(settings.BASE_DIR, 'static', file_name)
                # with open(file_path, 'rb') as file:
                #     df = pd.read_csv(file)
                df = fetch_all_opusnet_data()
                geojson_OPUSNET_str = generate_OPUSNET_geojson(df,input_date)
                geojson_OPUSNET_data = json.loads(geojson_OPUSNET_str)
                return Response(geojson_OPUSNET_data,status=status.HTTP_200_OK)
        
        except ValueError:
            return Response({"error": "Invalid date format"}, status=status.HTTP_400_BAD_REQUEST)
        
        except Exception as e:
            # Handle any errors that occur during processing
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
