python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade osmium boto3
python3 pull_osm_data.py
