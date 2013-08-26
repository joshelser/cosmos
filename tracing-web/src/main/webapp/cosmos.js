MOST_RECENT_URL = "rest/cosmos/recent"
MOST_RECENT_QUERY_STRING = "?num="

CONTENT_CLICKABLES = {'#most_recent_5':5, '#most_recent_10':10, '#most_recent_15':15};

digits = function(str) {
    return str.replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,"); 
}

render_most_recent = function(js, render_to) {
    html = ''

    for (var i = 0; i < js.length; i++) {
        beginDateTime = new Date(js[i].begin).toLocaleString();

        html += '<div>'
        html += '<p><span style="font-weight:bold">' + js[i].uuid + '</span> began at ' + beginDateTime + '</p>'
        html += '<ol>'
        total_duration = 0
        for (var j = 0; j < js[i].regionTimings.length; j++) {
            desc = js[i].regionTimings[j].description
            duration = js[i].regionTimings[j].duration
            total_duration += duration

            html += '<li>' + desc + ' = ' + digits(duration.toString()) + 'ms</li>'
        }
        html += '</ol>'

        if (total_duration > 0) {
            html += '<p style="padding-left:18px">Total = ' + digits(total_duration.toString()) + 'ms</p>'
        }

    }

    render_to.html(html)
}


$(document).ready(function() {
    content = $('#content')
    for (index in CONTENT_CLICKABLES) {
        $(index).click(function() {
            id = '#'+$(this)[0].id
            resp = $.get(MOST_RECENT_URL + MOST_RECENT_QUERY_STRING + CONTENT_CLICKABLES[id], {}, 
                function(data, textStatus, jqXHR) {
                    render_most_recent(data, content)
                }
            );
        });
    }

});
