/**
 * Created by maayanlab on 9/16/16.
 */
$(document).ready(function(){

    Dropzone.autoDiscover = false;

    Dropzone.options.drop = {
        method: 'post',
        paramName: 'file',
        parallelUploads: 1,
        dictDefaultMessage: "Drag fastq/fastq.gz files here for upload. Only fastq/fastq.gz files supported at this point.",
        clickable: true,
        enqueueForUpload: true,
        maxFilesize: 6000000,
        uploadMultiple: false,
        addRemoveLinks: false,
        acceptedFiles: ".fastq,.fastq.gz",

        init: function() {
            this.on("addedfile", function(file) {
                alert("cool");
                this.emit("thumbnail", file, "images/dna.png");
            }),
            this.on("sending", function(form, xhr) {
                console.log($("#drop").html());
                upload();
            }),
            this.on("complete", function(file, response) {
                this.removeFile(file);
                alert("upload complete");
            })
        }
    }

    $('#drop').dropzone();

});
