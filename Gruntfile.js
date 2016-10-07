require('shelljs/global');

module.exports = function(grunt) {

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
//          curly: true,
//          eqeqeq: true,
          freeze: true,
          futurehostile: true,
          ignores: [
            "kive/**/*.min.js",
            "kive/**/*.pack.js",
            "kive/**/imagediff.js",
            "kive/**/jsil8n.js",
            "kive/**/jasmine.js",
            "kive/**/system.js",
            "kive/**/noxss.js"
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
            src: [ 'kive/pipeline/static/pipeline/drydock_objects.js' ],
            actions: [
                {
                    name: 'inlinedpngs',
                    search: /\/\*inline ([^\*\r\n]+):\*\/"[^"\r\n]+"/g,
                    replace: function(match, p1) {
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
                                img = exec(cmd, { silent: true }).output.replace(/\s/g, '');
                            } else {
                                console.log('Inlining ' + p1 + '...');
                                img = grunt.file.read(filename, { encoding: "base64" });
                            }
                            return "\/*inline " + p1 + ":*\/\"" + img + "\"";
                        }
                    }
                }
            ]
        }
    }
//     uglify: {
//       options: {
//         banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
//       },
//       build: {
//         src: 'src/<%= pkg.name %>.js',
//         dest: 'build/<%= pkg.name %>.min.js'
//       }
//     }
  });

  // Load plugins
  var plugins = [
    'grunt-contrib-uglify',
    'grunt-contrib-jshint',
    'grunt-regex-replace',
    "grunt-tslint"
  ];
  for (var i = 0; i < plugins.length; i++) {
    grunt.loadNpmTasks(plugins[i]);
  }

  // Default task(s).
  grunt.registerTask('default', ['jshint']);
  grunt.registerTask('pngicons', ['regex-replace']);
};