document.addEventListener("DOMContentLoaded", function(event) {
    "use strict";
    
    var id = document.getElementById.bind(document);
    
    // Helper function that formats the file sizes
    var filesize_units = [ 'B', 'KB', 'MB', 'GB', 'TB', 'PB' ];
    function formatFileSize(bytes) {
        if (typeof bytes !== 'number') {
            return '';
        }
        var i = 0;
        while (bytes > 1024 && filesize_units[i + 1]) {
            bytes /= 1024;
            i++;
        }
        if (i) {
            bytes = bytes.toFixed(2);
        }
        
        return bytes + ' ' + filesize_units[i];
    }
    
    if (!(window.File && window.FileReader && window.FileList && window.Blob)) {
        console.error('Unsupported browser');
    }
    
    // Hide loading gif
    var loading = id('loading');
    loading.style.display = 'none';
    
    /*
     * Live uploading not currently implemented.
     * If implementing, remember that jQuery has been removed from this page.
    
    var $uploadProgressTable = $('#uploadProgressTable');
    id("id_dataset_files").addEventListener('change', function() {
        // clear contents of Upload Progress Table
        
        $uploadProgressTable.find('tbody').remove();
        
        var fileList = id('id_dataset_files').files;
        console.log('changed', fileList);
        var rows = [];
        for (var i = 0; i < fileList.length; i++) {
            var f = fileList[i];
            rows.push($('<tr>').append([
                $('<td>').text(f.name),
                $('<td>').text(formatFileSize(f.size)),
                $('<td>'), $('<td>'),
                $('<td>'), $('<td>')
            ]));
        }
        $uploadProgressTable.append(rows);
    });
    
    */
    
    var archiveSubmit = id('archiveSubmit');
    var bulkSubmit = id('bulkSubmit');
    
    if (archiveSubmit) {
        archiveSubmit.addEventListener('submit', function () {
            // disable the submit button until after the files have been uploaded and the datasets have been created.
            archiveSubmit.setAttribute("disabled", true);
        
            // Indicate to user that we are going to wait a long time
            loading.style.display = 'block';
    
            // Update mode? (not currently implemented)
            // if (!$("#editButton").is(":disabled")) {
            //     id("datasetArchiveForm").action = "/datasets_update_bulk";
            // }
        });
    }
    
    if (bulkSubmit) {
        bulkSubmit.addEventListener('submit', function () {
            // disable the submit button until after the files have been uploaded and the datasets have been created.
            bulkSubmit.setAttribute("disabled", true);
        
            // Indicate to user that we are going to wait a long time
            loading.style.display = 'block';
        
            // Update mode? (not currently implemented)
            // if (!$("#editButton").is(":disabled")) {
            //     id("datasetBulkForm").action = "/datasets_update_bulk";
            // }
        });
    }
});