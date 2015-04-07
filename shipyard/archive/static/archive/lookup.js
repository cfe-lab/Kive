$(document).ready(function(){
    var self = this;
    var doc = $(document);

    self.dzone = $("#dropzone");
    self.file_io = (window.File && window.FileReader && window.FileList && window.Blob);
    self.md5_sum = null;

    function handleFiles(files) {
        var reader = new FileReader();
        reader.onloadend = function(e) {
            self.md5_sum = md5(e.target.result);
            $('#continuebtn').removeAttr('disabled');
        };
        reader.readAsBinaryString(files[0]);
        console.log(files[0]);
    }

    // Continue setup
    self.dzone.on('dragenter', function (e)
    {
        e.stopPropagation();
        e.preventDefault();
        self.dzone.css('border', '2px solid #0B85A1');
    });
    self.dzone.on('dragover', function (e)
    {
         e.stopPropagation();
         e.preventDefault();
    });
    self.dzone.on('drop', function (e)
    {
         self.dzone.css('border', '2px dotted #0B85A1');
         e.preventDefault();
         var files = e.originalEvent.dataTransfer.files;

         //We need to send dropped files to Server
         handleFiles(files);
    });

    // Override the drag handler for
    // the page
    doc.on('dragenter', function (e)
    {
        e.stopPropagation();
        e.preventDefault();
    });
    doc.on('dragover', function (e)
    {
      e.stopPropagation();
      e.preventDefault();
      self.dzone.css('border', '2px dotted #0B85A1');
    });
    doc.on('drop', function (e)
    {
        e.stopPropagation();
        e.preventDefault();
    });
    $('#continuebtn').on('click', function(e){
        window.location.href = "/datasets_lookup/" + self.md5_sum;
    });

});