const webpackConfig = require('./webpack.config');

module.exports = function(config) {
    const cfgObj = {
        basePath: 'kive',
        browsers: ['Chrome', 'ChromeHeadless'],
        browserConsoleLogOptions: { level: "error" }, // chrome log level
        customHeaders: [{
            match: '.*',
            name: 'Set-Cookie',
            value: 'csrftoken=csrfdummytoken'
        }],
        frameworks: ['jasmine-ajax', 'jasmine-jquery', 'jasmine'],
        files: [ 'tests.ts' ],
        logLevel: config.LOG_INFO, // karma log level
        mime: { 'text/x-typescript': ['ts','tsx'] }, // required for typescript
        plugins: [
            '@metahub/karma-jasmine-jquery', // forked repo uses jasmine up to 2.5.2
            "karma-*",
        ],
        preprocessors: { 'tests.ts': ['webpack', 'sourcemap'] },
        port: 9876, // karma web server port
        proxies: {}, // will be filled programmatically
        reporters: ['progress', /* 'spec', */'kjhtml'],
        webpack: Object.assign({},
            webpackConfig,
            { entry: undefined, node: { fs: 'empty' } } // remove entry points for bundling
        ),
        webpackMiddleware: { quiet: true } // webpack-dev-middleware configuration
    };
    // simulate the way Django structures directories for static file serving
    serveDjangoPath(cfgObj, {
        'container': ['templates', 'static', 'test_assets'],
        'portal': ['static']
    });
    cfgObj.proxies['/portal/'] = '/base/portal/';
    config.set(cfgObj);
};

function serveDjangoPath(cfgObj, pathDefs) {
    for (let app in pathDefs) if (pathDefs.hasOwnProperty(app)) {
        let dirs = pathDefs[app];
        for (let dir of dirs) {
            let appdir = `${dir}/${app}`;
            cfgObj.files.push({
                pattern: `${app}/${appdir}/**/*`,
                included: false
            });
            cfgObj.proxies[`/${appdir}/`] =
                `/base/${app}/${appdir}/`;
        }
    }
}