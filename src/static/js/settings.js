
function form_to_JSON(form) {
    var array = $(form).serializeArray();
    var json = {};
    $.each(array, function () {
        json[this.name] = this.value || "";
    });

    return json;
}


function submit_override() {
    $(".some_form").on("submit", function (event) {

        // Prevent default submit-event.
        event.preventDefault();

        // Get form and transform to JSON
        var json = form_to_JSON(this);

        $.ajax({
            async: false,
            type: "POST",
            url: event.currentTarget.getAttribute("action"),
            data: JSON.stringify(json),
            dataType: "json",
            contentType: "application/json; charset=utf-8",
            xhrField: {withCredentials: true},
            success: function () {
                setTimeout(
                    location.reload(true), 1000
                );
            },
            error: function () {
                setTimeout(
                    location.reload(true), 1000
                );
            }
        });

        return false;
        // Reloads the page
    });
}


$(document).ready(function() {
    submit_override();
});
