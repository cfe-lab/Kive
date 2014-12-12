export DJANGO_SETTINGS_MODULE=shipyard.settings
./nukeDB.bash
python2.7 manage.py loaddata initial_pipeline
python2.7 setup_test_pipeline.py
