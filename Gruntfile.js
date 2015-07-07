require('shelljs/global');

module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    jshint: {
        files: [
            "kive/tests.js",
            "kive/archive/*.js",
            "kive/pipeline/static/pipeline/drydock.js",
            "kive/pipeline/static/pipeline/drydock_objects.js",
            "kive/pipeline/static/pipeline/pipeline_families.js",
            "kive/pipeline/static/pipeline/pipeline_revise.js",
            "kive/pipeline/static/pipeline/pipeline_load.js",
            "kive/portal/static/portal/permissions.js",
            "kive/sandbox/static/sandbox/choose*.js"
        ],
        options: {
//          curly: true,
//          eqeqeq: true,
          freeze: true,
          futurehostile: true,
//          maxdepth: 6,
          maxerr: 20,
//          nocomma: true,
          nonbsp: true,
//          undef: true,
//          unused: true,
          globals: {
            jQuery: true
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
    'grunt-regex-replace'
  ];
  for (var i = 0; i < plugins.length; i++) {
    grunt.loadNpmTasks(plugins[i]);
  }

  // Default task(s).
  grunt.registerTask('default', ['jshint']);
  grunt.registerTask('pngicons', ['regex-replace']);
};