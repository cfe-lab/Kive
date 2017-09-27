(window as any).$ = $;
require('@portal/noxss.js');
require('./PipelineRunTable.ts');
require('@portal/ajaxsearchfilter.js');
declare var permissions: { [key: string]: any };
declare var AjaxSearchFilter: any;

var IS_USER_ADMIN = false; // Never show admin tools on this page
var table = new permissions.PipelineRunTable(
    $('#pipeline_families'),
    IS_USER_ADMIN,
    $(".navigation_links")
);
var asf = new AjaxSearchFilter(table);
asf.reloadTable();