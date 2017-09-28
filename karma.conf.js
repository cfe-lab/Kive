var webpackConfig = require('./webpack.config');

module.exports = function(config) {
    var cfgObj = {
        frameworks: ['jasmine-ajax', 'jasmine-jquery', 'jasmine'],
        basePath: 'kive',
        files: [
            'tests.ts',
            { pattern: '**/*.js.map', included: false } // serve source-maps
        ],
        preprocessors: {
            'tests.ts': ['webpack', 'sourcemap']
        },
        reporters: ['progress', /* 'spec', */'kjhtml'],
        port: 9876,  // karma web server port
        colors: true,
        logLevel: config.LOG_INFO,
        browsers: ['Chrome', 'ChromeHeadless'],
        autoWatch: true,
        concurrency: Infinity,
        mime: { 'text/x-typescript': ['ts','tsx'] }, // required for typescript usage
        plugins: [
            '@metahub/karma-jasmine-jquery', // forked repo uses jasmine up to 2.5.2
            "karma-*",
        ],
        webpack: Object.assign({},
            webpackConfig,
            {
                entry: undefined, // remove entry points for bundling
                node: { fs: 'empty' }
            }
        ),
        webpackMiddleware: {
            // webpack-dev-middleware configuration
            quiet: true
        }
    };

    // simulate the way Django structures directories
    serveDjangoPath(cfgObj, {
        'pipeline': ['templates', 'static', 'test_assets'],
        'portal': ['static'],
    });
    cfgObj.proxies['/portal/'] = '/base/portal/';
    config.set(cfgObj);
};

function serveDjangoPath(cfgObj, pathDefinitions) {
    if (!cfgObj.hasOwnProperty('proxies')) {
        cfgObj.proxies = {};
    }
    for (let dir in pathDefinitions) if (pathDefinitions.hasOwnProperty(dir)) {
        let subDirs = pathDefinitions[dir];
        for (let subDir of subDirs) {
            cfgObj.files.push(
                { pattern: [ dir, subDir, dir, '**/*' ].join('/'), watched: true, served: true, included: false }
            );
            cfgObj.proxies[ ['', subDir, dir, ''].join('/') ] = ['/base', dir, subDir, dir, ''].join('/');
        }
    }
}