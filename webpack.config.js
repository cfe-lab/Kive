const webpack = require('webpack');
// const UglifyJSPlugin = require('uglifyjs-webpack-plugin');

module.exports = {
    entry: {
        pipeline_add: "./kive/pipeline/static/pipeline/pipeline_add.ts"
    },
    output: {
        filename: "[name].bundle.js",
        path: __dirname + "/kive/portal/static/portal"
    },

    // Enable sourcemaps for debugging webpack's output.
    devtool: "inline-source-map",

    plugins: [
        // new webpack.DefinePlugin({
        //     'process.env': {
        //         NODE_ENV: JSON.stringify('production')
        //     }
        // }),
        // new UglifyJSPlugin(),

        new webpack.ProvidePlugin({
            $: 'jquery',
            jQuery: 'jquery'
        }),

        new webpack.SourceMapDevToolPlugin({
            filename: null, // if no value is provided the sourcemap is inlined
            test: /\.(ts|js)($|\?)/i // process .js and .ts files only
        })
    ],

    resolve: {
        // Add '.ts' and '.tsx' as resolvable extensions.
        extensions: [".webpack.js", ".web.js", ".ts", ".tsx", ".js", ".css"]
    },

    module: {
        rules: [
            // All files with a '.ts' or '.tsx' extension will be handled by 'awesome-typescript-loader'.
            { test: /\.tsx?$/, loader: "awesome-typescript-loader" },

            {
                test: /\.css$/,
                use: [
                    { loader: "style-loader" },
                    { loader: "css-loader" }
                ]
            },

            {
                test: /\.s[ac]ss$/,
                use: [
                    { loader: "style-loader" }, // creates style nodes from JS strings
                    { loader: "css-loader" },   // translates CSS into CommonJS
                    { loader: "sass-loader" }   // compiles Sass to CSS
                ]
            }
        ]
    }
};