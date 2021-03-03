const webpack = require('webpack');
const TerserPlugin = require('terser-webpack-plugin');
const { TsConfigPathsPlugin } = require('awesome-typescript-loader');

module.exports = {
    entry: {
        container_content: "./kive/container/static/container/container_content.ts"
    },
    output: {
        filename: "[name].bundle.js",
        path: __dirname + "/kive/portal/static/portal"
    },

    // Enable sourcemaps for debugging webpack's output.
    // Suggested: eval-source-map when debugging, nosources-source-map in production
    devtool: "nosources-source-map",

    plugins: [
        new webpack.DefinePlugin({
            'process.env': {
                NODE_ENV: JSON.stringify('production')
            }
        }),

        new webpack.ProvidePlugin({
            $: 'jquery',
            jQuery: 'jquery'
        }),

        new webpack.SourceMapDevToolPlugin({
            filename: null, // if no value is provided the sourcemap is inlined
            test: /\.(ts|js)($|\?)/i // process .js and .ts files only
        })

    ],
    optimization: {
        minimize: true,
        minimizer: [new TerserPlugin()],
    },

    resolve: {
        // Add '.ts' and '.tsx' as resolvable extensions.
        extensions: [".webpack.js", ".web.js", ".ts", ".tsx", ".js", ".css"],
        plugins: [ new TsConfigPathsPlugin() ],
        fallback: {fs: false}
    },

    module: {
        rules: [
            // All files with a '.ts' or '.tsx' extension will be handled by 'awesome-typescript-loader'.
            { test: /\.tsx?$/, loader: "awesome-typescript-loader" },
            { test: /(permissions|ajaxsearchfilter|noxss|md5|choose_inputs)\.js$/, loader: 'script-loader' },
            { test: /\.css$/, use: [ "style-loader", "css-loader" ] },
            {
                test: /\.s[ac]ss$/,
                use: [
                    "style-loader", // creates style nodes from JS strings
                    "css-loader",   // translates CSS into CommonJS
                    "sass-loader"   // compiles Sass to CSS
                ]
            }
        ]
    }
};