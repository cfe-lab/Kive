cd ~/git/Shipyard/shipyard
./nukeDB.bash
python manage.py loaddata demo
cp -R FixtureFiles/demo/* .
