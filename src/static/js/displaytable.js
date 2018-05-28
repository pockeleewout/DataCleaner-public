// ES6 - doesn't work in browser

const fillTableRows = () => {
    fetch('url')
        .then(res => res.json())
        .then(json => {
            const table = $('#id'); //change id to the right one
            json.content.rows.forEach(row => {
                const tableRow = document.createElement('tr');

                row.forEach(rowElement => {
                    const column = document.createElement('td');
                    column.append(document.createTextNode(rowElement));
                    tableRow.append(column);
                });

                table.append(tableRow);
            });
        });
}

window.onload = () => fillTableRows();


// ES5 - works in browser
'use strict';

var fillTableRows = function fillTableRows() {
    fetch('url').then(function (res) {
        return res.json();
    }).then(function (json) {
        var table = $('#id'); //change id to the right one
        json.content.rows.forEach(function (row) {
            var tableRow = document.createElement('tr');

            row.forEach(function (rowElement) {
                var column = document.createElement('td');
                column.append(document.createTextNode(rowElement));
                tableRow.append(column);
            });

            table.append(tableRow);
        });
    });
};

window.onload = function () {
    return fillTableRows();
};
