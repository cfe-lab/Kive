const puppeteer = require('puppeteer');

puppeteer.launch().then(function(browser) {
    browser.newPage().then(function(page) {
        var url = 'file://' + __dirname + '/kive/SpecRunner.html';
        page.goto(url).then(function() {
            page.evaluate(function() {
                var summary = $('span.bar.failed').text(),
                    report = [];
                if ( ! summary.length) {
                    report.push($('span.bar.passed').text())
                }
                else {
                    report.push(summary);
                    $('div.spec-detail.failed').each(function() {
                        report.push($('div.result-message', this).text());
                    });
                }
                return report;
            }).then(function(result) {
                result.forEach(function(message) {
                    console.info(message);
                });
                if (result.length > 1) {
                    process.exitCode = 1;
                }
                browser.close();
            })
        });
    });
});
