cd ~/git/Shipyard/shipyard
./nukeDB.bash
python2.7 manage.py loaddata demo
cp -R FixtureFiles/demo/* .
