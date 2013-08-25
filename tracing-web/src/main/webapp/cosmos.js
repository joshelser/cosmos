MOST_RECENT_URL = "rest/cosmos/recent"
MOST_RECENT_QUERY_STRING = "?num="

CONTENT_CLICKABLES = {'#most_recent_5':5, '#most_recent_10':10, '#most_recent_15':15};

render_most_recent = function(js, render_to) {
    html = ''

    for (var i = 0; i < js.length; i++) {
        html += '<div>'
        html += '<p>' + js[i].uuid + '</p>'
        html += '<p>' + js[i].begin + '</p>'
        html += '<ol>'
        for (var j = 0; j < js[i].regionTimings.length; j++) {
            html += '<li>' + js[i].regionTimings[j].description
                    + ' = ' + js[i].regionTimings[j].duration
                    + 'ms</li>'
        }
        html += '</ol>'
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
