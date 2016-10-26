var bs = require("browser-sync").create();

// Start a Browsersync static file server
bs.init({
    server: {
        baseDir: "./kive",
        directory: true,
        routes: {
            "/tests": "kive/portal",
            "/static/portal": "kive/portal/static/portal",
            "/static/sandbox": "kive/sandbox/static/sandbox",
            "/static/method": "kive/method/static/method",
            "/static/pipeline": "kive/pipeline/static/pipeline",
            "/static/metadata": "kive/metadata/static/metadata",
            "/static/librarian": "kive/librarian/static/librarian",
            "/templates/portal": "kive/portal/templates/portal",
            "/templates/sandbox": "kive/sandbox/templates/sandbox",
            "/templates/method": "kive/method/templates/method",
            "/templates/pipeline": "kive/pipeline/templates/pipeline",
            "/templates/metadata": "kive/metadata/templates/metadata",
            "/templates/librarian": "kive/librarian/templates/librarian"
        }
    }
});