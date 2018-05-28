
//Clone the hidden element and shows it
$(".add-one").click(function(){
    $(".dynamic-element").first().clone().appendTo(".dynamic-stuff").show();

    // Take the value of both table selects and use it as an id, which will get you the correct select menu.
    // Clone that select menu and append it to the new dynamic element.
    $(".select_1").last().append($("#" + $("#table_1").val()).clone())
                         .removeClass("select_1").addClass("column_select_1");
    $(".select_2").last().append($("#" + $("#table_2").val()).clone())
                         .removeClass("select_2").addClass("column_select_2");
    $(".name_joined").last().removeClass("name_joined").addClass("joined_name");

    attach_delete();
});


//Attach functionality to delete buttons
function attach_delete(){
    $(".delete").off().click(function() {
        $(this).closest(".form-group").remove();
    });
}


$("#table_1").change(function() {
    $(".column_select_1").empty().append($("#" + $("#table_1").val()).clone());
});


$("#table_2").change(function() {
    $(".column_select_2").empty().append($("#" + $("#table_2").val()).clone());
});


function getJSON(form) {
    var json = {
        "db_id": $(form).data("id"),
        "table_1": $("#table_1").val(),
        "table_2": $("#table_2").val(),
        "columns_1": [],
        "columns_2": [],
        "name": $("#table_name")
    };

    $(".column_select_1").each(function(index, object) {
        json.columns_1.push($(object).find("select").val());
    });
    $(".column_select_2").each(function(index, object) {
        json.columns_2.push($(object).find("select").val());
    });

    return json;
}


function submit_override() {
    $(".join_form").on("submit", function (event) {
        event.preventDefault();

        var json = getJSON(this);

        $.ajax({
            async: false,
            type: "POST",
            url: event.currentTarget.getAttribute("action"),
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
    });
}


$(document).ready(function () {
    $(".selectpicker").selectpicker();
    $(".bootstrap-select").click(function () {
         $(this).addClass("open");
    });

    submit_override();
});
