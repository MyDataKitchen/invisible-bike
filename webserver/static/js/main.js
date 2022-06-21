function ajax(){
    var xmlhttp = new XMLHttpRequest();
    var url = "http://0.0.0.0:8080/api/1.0/data";
    xmlhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            let data = this.response;
            marker(data['data']);
        };
    };

    xmlhttp.open("GET", url, true);
    xmlhttp.setRequestHeader('Content-Type','application/x-www-form-urlencode');
    xmlhttp.responseType = 'json';
    xmlhttp.send();

};

function marker(data){
    var locations = data
    var map = L.map('map').setView([25.044878, 121.543877], 13.4);
    mapLink =
        '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="http://cartodb.com/attributions">CartoDB</a>';
        L.tileLayer(
        'https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}.png', {
        attribution: '©OpenStreetMap, ©CartoDB',
        maxZoom: 18,
        }).addTo(map);

    for (var i = 0; i < locations.length; i++) {
        marker = new L.marker([locations[i][1], locations[i][2]])
        .bindPopup(locations[i][0])
        .addTo(map);
    }
};

setInterval(function(){
    ajax();
    map.invalidateSize();
}, 1000)


