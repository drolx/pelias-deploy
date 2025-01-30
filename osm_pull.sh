python -m venv venv
source venv/bin/activate
pip install --upgrade osmium boto3
python pull_osm_data.py
