var webpackConfig = require('./webpack.config');
module.exports = function(config) {
    config.set({
        frameworks: ['jasmine'],
        files: [
            'kive/pipeline/tests.ts'
        ],
        preprocessors: {
            'kive/pipeline/tests.ts': ['webpack']
        },
        reporters: ['progress'],
        port: 9876,  // karma web server port
        colors: true,
        logLevel: config.LOG_INFO,
        browsers: ['Chrome', 'ChromeHeadless', 'MyHeadlessChrome'],
        autoWatch: false,
        // singleRun: false, // Karma captures browsers, runs the tests and exits
        concurrency: Infinity,
        customLaunchers: {
            MyHeadlessChrome: {
                base: 'ChromeHeadless',
                flags: ['--disable-translate', '--disable-extensions', '--remote-debugging-port=9223']
            }
        },
        webpack: {
            devtool: webpackConfig.devtool,
            resolve: webpackConfig.resolve,
            module: webpackConfig.module,
            node: {
                fs: 'empty'
            }
        },
        webpackMiddleware: {
            // webpack-dev-middleware configuration
            quiet: true
            // and use stats to turn off verbose output
            // stats: {
                // options i.e.
                // chunks: false
            // }
        }
    })
};