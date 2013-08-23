MOST_RECENT_URL = "rest/cosmos/recent"

$(document).ready(function() {
    
    $('#most_recent').click(function() {
        resp = $.get(MOST_RECENT_URL, {}, function(data, textStatus, jqXHR) {
            console.log(data)

            //results = jQuery.parseJSON(data)
            results = data
            console.log("derp")
            console.log(results)
            html = ''

            for (var i = 0; i < results.length; i++) {
                html += '<div>'
                html += '<p>' + results[i].uuid + '</p>'
                html += '<p>' + results[i].begin + '</p>'
                html += '<ol>'
                for (var j = 0; j < results[i].regionTimings.length; j++) {
                    html += '<li>' + results[i].regionTimings[j].description
                            + ' = ' + results[i].regionTimings[j].duration
                            + 'ms</li>'
                }
                html += '</ol>'
            }

            $('#content').html(html)
        });
    });

});
