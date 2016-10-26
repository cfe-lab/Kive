require('shelljs/global');


module.exports = function (grunt) {
    
    // Project configuration.
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        tslint: {
            options: {
                configuration: "tslint.json"
            },
            files: {
                src: [
                    "kive/**/*.ts"
                ]
            }
        },
        jshint: {
            files: [
                "kive/*.js",
                "kive/**/*.js"
            ],
            options: {
                freeze: true,
                futurehostile: true,
                ignores: [
                    "kive/**/*.min.js",
                    "kive/**/*.pack.js",
                    "kive/**/imagediff.js",
                    "kive/**/jsil8n.js",
                    "kive/**/jasmine.js",
                    "kive/**/jasmine-jquery.js",
                    "kive/**/system.js",
                    "kive/**/noxss.js",
                    "kive/pipeline/**/*.js",
                    "kive/pipeline/*.js",
                    "kive/sandbox/static/sandbox/PipelineRunTable.js",
                    "kive/sandbox/static/sandbox/view_run.js"
                ],
                maxerr: 20,
                nonbsp: true,
                globals: {
                    jQuery: true,
                    "$": true,
                    "window": true,
                    "permissions": true
                }
            }
        },
        "regex-replace": {
            inlinedpngs: {
                src: ['kive/pipeline/static/pipeline/drydock_objects.ts'],
                actions: [
                    {
                        name: 'inlinedpngs',
                        search: /\/\*inline ([^\*\r\n]+):\*\/"[^"\r\n]*"/g,
                        replace: function (match, p1) {
                            var filename = 'raw_assets/' + p1 + '.png';
                            if (!test('-e', filename)) {
                                grunt.fail.fatal('"' + filename + '" not found', 3);
                            } else {
                                if (which('base64')) {
                                    var cmd = 'cat "' + filename + '" | ';
                                    if (which('pngquant')) {
                                        console.log('Inlining ' + p1 + ' using pngquant...');
                                        cmd += 'pngquant -s1 - | ';
                                    } else {
                                        console.log('Inlining ' + p1 + '...');
                                    }
                                    cmd += 'base64';
                                    img = exec(cmd, {silent: true}).output.replace(/\s/g, '');
                                } else {
                                    console.log('Inlining ' + p1 + '...');
                                    img = grunt.file.read(filename, {encoding: "base64"});
                                }
                                return "\/*inline " + p1 + ":*\/\"" + img + "\"";
                            }
                        }
                    }
                ]
            }
        }
    });
    
    // Load plugins
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-regex-replace');
    grunt.loadNpmTasks('grunt-tslint');
    
    // Default task(s).
    grunt.registerTask('default', ['jshint', 'tslint']);
    grunt.registerTask('pngicons', ['regex-replace']);
};
