

function display_databases()
{
    $.ajax({
        async: false,
        type: 'GET',
        url: $('#url_databases').data('url'),
        dataType: "json",
        contentType: "application/json; charset=utf-8",
        xhrField: {withCredentials: true},
        success: function (data) {

            var baseUrl = $('#url_view').data('url');

            for (var i = 0; i < data.size; i++)
            {
                var adjDataUrl = baseUrl.replace('NIL', String(data.data[i].id));
                var body = $('body').find('._hidden_src').clone();

                body.removeAttr('class');

                body.find('#url_field').append(
                    "<a href=" + String(adjDataUrl) + ">" +
                    "<div id=\"linkcolor\" style=\"height:100%;width:100%\">" +
                    data.data[i].name +
                    "</div>" +
                    "</a>"
                );

                body.find('#desc_field').append(
                    data.data[i].description
                );

                body.find('#owner_field').append(
                    "Coming soon..."
                );

                body.find('#users_modal_field').append(
                    "<p class=\"edit_db\" data-placement=\"top\" data-toggle=\"tooltip\" title=\"Users\" data-id=" +
                    data.data[i].id + ">" +
                    "<button class=\"btn btn-info btn-sm\" data-title=\"Users\"" +
                    "data-toggle=\"modal\"" +
                    "data-target=\"#users\"><span class=\"fas fa-users\"></span>" +
                    "</button>" +
                    "</p>"
                );

                body.find('#edit_modal_field').append(
                    "<p class=\"edit_db\" data-placement=\"top\" data-toggle=\"tooltip\" title=\"Edit\" data-id=" +
                    data.data[i].id + ">" +
                    "<button class=\"btn btn-primary btn-sm\" data-title=\"edit\"" +
                    "data-toggle=\"modal\"" +
                    "data-target=\"#edit\"><span class=\"fas fa-pencil-alt\"></span>" +
                    "</button>" +
                    "</p>"
                );

                body.find('#delete_modal_field').append(
                    "<p class=\"edit_db\" data-placement=\"top\" data-toggle=\"tooltip\" title=\"Delete\" data-id=" +
                    data.data[i].id + ">" +
                    "<button class=\"btn btn-danger btn-sm\" data-title=\"Delete\"" +
                    "data-toggle=\"modal\"" +
                    "data-target=\"#delete\"><span class=\"fas fa-trash-alt\"></span>" +
                    "</button>" +
                    "</p>"
                );

                body.find('#download_modal_field').append(
                    "<p data-placement=\"top\" data-toggle=\"tooltip\" title=\"Download\" data-id=" +
                    data.data[i].id + ">" +
                    "<button type=\"submit\" class=\"btn btn-success btn-sm\" data-title=\"Download\" data-toggle=\"modal\" data-target=\"#download\">\n" +
                    "<span class=\"fas fa-download\"></span>\n" +
                    "</button>" +
                    "</p>"
                );


                $(body).appendTo("#mytable");
            }

            $('._hidden_src').remove();


        },
        error: function () {

        }
    });
}

$(document).on("click", ".edit_db", function () {
    var data_id = $(this).data('id');
    $(".form-group #data_id").val(data_id);
    $($(this).data('target')).modal('show');
});

function FormToJSON(form) {
    var array = jQuery(form).serializeArray();
    var json = {};
    jQuery.each(array, function () {
        json[this.name] = this.value || "";
    });

    return json;
}

function submit_override() {
    $(".json_form").on("submit", function(event)
    {
        // Prevent default submit-event.
        event.preventDefault();

        // Get form and transform to JSON
        var form = this;
        var json = FormToJSON(form);
        var adjDataUrl = event.currentTarget.getAttribute("action").replace
        ('NIL', $("#data_id").val());

        $.ajax({
            async: false,
            type: event.currentTarget.getAttribute("method"),
            url: adjDataUrl,
            data: JSON.stringify(json),
            dataType: "json",
            contentType: "application/json; charset=utf-8",
            xhrField: { withCredentials: true },
            success: function()
            {
                setTimeout(
                    location.reload(true), 1000
                );
            },
            error: function()
            {
                setTimeout(
                    location.reload(true), 1000
                );
            }
        });

        return false;
    });
}


function init()
{
    submit_override();
    display_databases();
}


$(document).ready(init());


