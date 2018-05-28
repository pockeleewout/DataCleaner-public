$(document).on("click", ".open-transformation", function () {
    var col_id = $(this).data('id');
    $(".form-group #colinfo").val(col_id);
    $($(this).data('target')).modal('show');
});


function changeNrInputs() {
    var inputs = document.getElementById('discr_range_inputs');
    var nr_bins = parseInt(document.getElementById('nr_bins').value);
    var nr_children = parseInt(inputs.getAttribute('data-value'));

    if (nr_bins < 2) {
        document.getElementById('nr_bins').value = 2;
        nr_bins = 2;
    }

    if (nr_bins > nr_children) {
        for (var i = nr_children; i < nr_bins; i++) {
            var name = 'input_' + i.toString();

            var new_div = document.createElement('div');
            new_div.id = name;

            var new_input = document.createElement('input', {is: name});
            new_input.className = 'form-control';
            new_input.type = 'number';
            new_input.name = name;
            new_input.step = 'any';
            new_input.required = true;

            new_div.append(new_input);
            inputs.append(new_div);
        }
    }
    else if (nr_bins < nr_children) {
        for (var j = nr_bins; j < nr_children; j++) {
            var to_remove = document.getElementById('input_' + j);
            inputs.removeChild(to_remove);
        }
    }
    inputs.setAttribute('data-value', nr_bins.toString())
}

function FormToJSON(form) {
    var array = jQuery(form).serializeArray();
    var json = {};
    jQuery.each(array, function () {
        json[this.name] = this.value || "";
    });

    return json;
}

function init_datatable() {
    jQuery('#example').DataTable({
        processing: true,
        searching: false,
        ordering: true,
        serverSide: true,
        ajax: {
            url: "/api/v1/table/" + table_id + "/content/",
            data: JSON.stringify,
            xhrFields: {
                withCredentials: true
            },
            type: "post",
            contentType: "application/json; charset=utf-8"
        }
    });
}

function submit_override() {
    $(".trans_form").on("submit", function (event) {

        // Prevent default submit-event.
        event.preventDefault();

        // Get form and transform to JSON
        var form = this;
        var json = FormToJSON(form);

        // Add database id to the data
        json["db_id"] = $(this).data("id");

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

function init() {
    init_datatable();
    submit_override();
}

$(document).ready(init());


/*
$(function() {

  $('#fade').click(function(){
    $('#fade').css('color','black');
    $('#fade2').css('color','gray');
  });

});
*/
/*
$(function() {

  $('#fade2').click(function(){
    $('#fade2').css('color','black');
    $('#fade').css('color','gray');
  });

});
*/
