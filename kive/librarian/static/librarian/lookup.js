$(document).ready(function(){
    var self = this;
    var doc = $(document);

    self.dzone = $("#dropzone");
    self.dels = $("#dropzone, .dragfix");
    self.in_counter = 0;

    self.file_io = (window.File && window.FileReader && window.FileList && window.Blob);
    self.md5_sum = null;
    self.ffname = null;
    self.ffsize = null;
    
    function handleFiles(files) {
        var reader = new FileReader();
        reader.onloadend = function(e) {
            self.md5_sum = md5(e.target.result);
	    self.ffname = files[0].name;
	    self.ffsize = files[0].size;
	    
            $('#no_file').hide();
            $('#file_loaded').show();

            $('#id_filename').text(files[0].name);
            $('#id_md5sum').text(self.md5_sum);
            $('#continuebtn').removeAttr('disabled');
        };
        reader.readAsBinaryString(files[0]);
    }

    // Continue setup
    self.dels.on('dragenter', function (e)
    {
        e.stopPropagation();
        e.preventDefault();
        self.in_counter++;
        self.dzone.addClass('dz_shadow');
    });
    self.dels.on('dragover', function (e)
    {
         e.stopPropagation();
         e.preventDefault();
    });
    self.dels.on('dragleave dragexit', function(e){
         e.stopPropagation();
         e.preventDefault();
         self.in_counter--;

         if(!self.in_counter)
            self.dzone.removeClass('dz_shadow');
    });
    self.dels.on('drop', function (e)
    {
        self.in_counter = 0;
        self.dzone.removeClass('dz_shadow');
        e.preventDefault();
        var files = e.originalEvent.dataTransfer.files;

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
    });
    doc.on('drop', function (e)
    {
        e.stopPropagation();
        e.preventDefault();
    });

    $('#filesel').on('click', function(e){
        e.preventDefault();
        e.stopPropagation();

        $('#upload_file').click();
    });

    $("#upload_file").on('change', function (e) {
        e.preventDefault();
        e.stopPropagation();
        if(self.file_io)
            handleFiles(e.target.files);
        $('#continuebtn').removeAttr('disabled');

    });

    //
    if(self.file_io){

        $('#continuebtn').on('click', function(e){
            e.stopPropagation();
            e.preventDefault();

            window.location.href = "/datasets_lookup/" + encodeURIComponent(self.ffname) + "/" + self.ffsize + "/" + self.md5_sum;
        });
    } else {
        $('#upload_file').show();
        $('#file_io_compat').hide();
    }
});
