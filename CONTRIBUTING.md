# Deploying a Release

See the project wiki for instructions on how to [start a production server][wiki].
Once you have set up your production server, this is how to deploy a new release:

1. Make sure the code works in your development environment. Run all the unit
    tests.
    
    ./manage.py test
    
2. Check that all the issues in the current milestone are closed.
3. [Create a release][release] on Github. Use "vX.Y" as the tag, where X.Y
    matches the version on the milestone. If you have to redo
    a release, you can create additional releases with tags vX.Y.1, vX.Y.2, and
    so on. Mark the release as pre-release until you finish deploying it.
4. TODO: Check whether there are problems with doing a deployment while a run
    is executing. Does restarting apache restart the run?
5. Get the code from Github onto the server.

        ssh user@server
        cd /usr/local/share/Kive/kive
        git fetch
        git checkout tags/vX.Y

6. Check if you need to set any new settings by running
    `diff kive/settings_default.py kive/settings.py`. Do the same
    comparison of `hostfile`.
7. Recreate the database as described in the Initialize Database section
    of README.md, and deploy the static files.
    
        ssh user@server
        cd /usr/local/share/Kive/kive
        ./manage.py migrate
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./manage.py collectstatic
        
8. TODO: Check whether an apache restart is needed. What about the fleet manager?

        ps aux | grep runfleet
        sudo kill <pid for runfleet>
        sudo /usr/sbin/apachectl restart
        sudo -u apache LD_LIBRARY_PATH=$LD_LIBRARY_PATH PATH=$PATH ./manage.py runfleet --workers 151 &>/dev/null &

9. Remove the pre-release flag from the release.
10. Close the milestone for this release, create one for the next release, and
    decide which issues you will include in that milestone.

[release]: https://help.github.com/categories/85/articles
[wiki]: https://github.com/cfe-lab/Kive/wiki/Starting-a-production-server-for-Shipyard-(Django)

# Unit tests

To run all the unit tests, run `./manage.py test`. Note that running the
full test suite can take around half an hour.

## Updating test fixtures

Fixtures are a Django feature which allow for test data to be
persistently stored in the database during development, to avoid having
to reload it every time a unit test is run. This is especially
convenient for unit tests which involve actually running a pipeline,
which can take a long time.

The fixtures which are used in our unit tests are created with the
custom command `./manage.py update_test_fixtures`. The code that is
executed for this command can be found in
`archive/management/commands/update_test_fixtures.py`. If this code or
any functions it calls are modified, the fixtures will need to be
re-created using the aforementioned command.

