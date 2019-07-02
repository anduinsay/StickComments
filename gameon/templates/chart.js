var margin = { top:0 , right:0 , bottom:20 , left:30 }

var screen_width;
var screen_height;

var chart_container_width;
var chart_container_height;

var chart_width;
var chart_height;


d3.csv("redditres.csv").then(d => chart(d));
var days = [...new Set(csv.map(d => d.Quantity))],
      years  = [...new Set(csv.map(d => d.Month))];


// Starting point of the script execution
define_chart_dimensions();

draw_chart_of_months(months);

window.onresize = function (){
  define_chart_dimensions();
  draw_chart_of_months(months);
}

function draw_chart_of_months(data){
  var horizontal_scale = d3.scaleBand().domain(data.map(function(item){return item.Month})).rangeRound([0,chart_width]);
  var vertical_scale = d3.scaleLinear().domain([0,d3.max(data, function(item){return item.Quantity})]).range([chart_height, 0]);
  var bar_width = chart_width / data.length - chart_width / data.length / 2;
  var bar_horizontal_margin = (chart_width / data.length - bar_width) / 2;
  var xAxis = d3.axisBottom(horizontal_scale);
  var yAxis = d3.axisLeft(vertical_scale);

  var chart = d3.select("#d3_wrapper svg").attr("width", chart_container_width).attr("height", chart_container_height)
    .select("g").attr("width", chart_width).attr("height", chart_height).attr("transform", "translate("+margin.left+", "+margin.top+")");

  d3.select(".x.axis").remove();
  chart.append("g").attr("class", "x axis").attr("transform", "translate(0,"+(chart_container_height - margin.bottom)+")").call(xAxis).call(adjustxAxisTextForMonths);
  d3.select(".y.axis").remove();
  chart.append("g").attr("class", "y axis").call(yAxis);

  var bar = chart.selectAll(".bar").data(data);

  var update = bar.select("rect").attr("x", function(d,i) {return horizontal_scale(d.Month) + bar_horizontal_margin})
    .attr("y", function(d){return vertical_scale(d.Quantity)})
    .attr("width", bar_width)
    .on("mouseover", function(d,i){
        d3.select(this.parentNode).append("text").attr("x", function(d,i) {return horizontal_scale(d.Month) + bar_horizontal_margin + bar_width/2 + 5}).text(d.Quantity).attr("y", function(d){return vertical_scale(d.Quantity) + 15});
      })
    .attr("height", function(d){ return chart_height - vertical_scale(d.Quantity)});

  var enter = bar.enter().append("g").attr("class", "bar");
  enter.append("rect").attr("x", function(d,i) {return horizontal_scale(d.Month) + bar_horizontal_margin})
    .attr("y", function(d){return vertical_scale(d.Quantity)})
    .attr("width", bar_width)
    .on("click", function(d, i) { month_selected(d)})
    .on("mouseover", function(d,i){
        d3.select(this.parentNode).append("text").attr("x", function(d,i) {return horizontal_scale(d.Month) + bar_horizontal_margin + bar_width/2 + 5}).text(d.Quantity).attr("y", function(d){return vertical_scale(d.Quantity) + 15});
      })
    .on("mouseout", function(d,i){
        d3.select(this.parentNode).selectAll("text").remove();
      })
    .transition().delay(function(d,i){return i * 10})
    .attr("height", function(d){ return chart_height - vertical_scale(d.Quantity)}).attr("class", "month_bar");

  var exit = bar.exit();
    exit.select("rect").transition().duration("1000").attr("height", 0);
}

function draw_chart_of_days(days){
  var horizontal_scale = d3.scaleBand().domain(days.map(function(item){return item.Day})).rangeRound([0,chart_width]);
  var vertical_scale = d3.scaleLinear().domain([0,d3.max(days, function(item){return item.Quantity})]).range([chart_height, 0]);
  var bar_width = chart_width / days.length - chart_width / days.length / 2;
  var bar_horizontal_margin = (chart_width / days.length - bar_width) / 2;
  var xAxis = d3.axisBottom(horizontal_scale);
  var yAxis = d3.axisLeft(vertical_scale);

  var chart = d3.select("#d3_wrapper svg").attr("width", chart_container_width).attr("height", chart_container_height)
    .select("g").attr("width", chart_width).attr("height", chart_height).attr("transform", "translate("+margin.left+", "+margin.top+")");

  var bar = chart.selectAll(".bar").data(days);

  d3.select(".x.axis").remove();
  chart.append("g").attr("class", "x axis").attr("transform", "translate(0,"+(chart_container_height - margin.bottom)+")").call(xAxis).call(adjustxAxisTextForDays);
  d3.select(".y.axis").remove();
  chart.append("g").attr("class", "y axis").call(yAxis);

  var g = bar.enter().append("g").attr("class", "bar");

  g.append("rect").attr("x", function(d,i) {return horizontal_scale(d.Day) + bar_horizontal_margin})
    .attr("y", function(d){return vertical_scale(d.Quantity)})
    .attr("width", bar_width)
    
    .on("click", function(d, i) { day_selected(d)})
    .on("mouseover", function(d,i){
        d3.select(this.parentNode).append("text").attr("x", function(d,i) {return horizontal_scale(d.Day) + bar_horizontal_margin + bar_width/2 + 5}).text(d.Quantity).attr("y", function(d){return vertical_scale(d.Quantity) + 15});
      })
    .on("mouseout", function(d,i){
        d3.select(this.parentNode).selectAll("text").remove();
      })

    

    .transition().delay(function(d,i){return i * 10})
    .attr("height", function(d){ return chart_height - vertical_scale(d.Quantity)})
    .attr("class", "day_bar");

  var exit = bar.exit();
    exit.select("rect").transition().duration("1000").attr("height", "0");
}

function define_chart_dimensions(){
  screen_width = get_screen_width_height().width;
  screen_height = get_screen_width_height().height;

  chart_container_width = define_chart_container_width(screen_width);
  chart_container_height = define_chart_container_height(screen_height);

  chart_width = chart_container_width - margin.right - margin.left;
  chart_height = chart_container_height - margin.top - margin.bottom;
}


function day_selected(day){
  draw_chart_of_days([]);
  setTimeout(function(){
    d3.selectAll(".chart g").remove();
    d3.select(".chart").append("g");
    if(day.Day=='Jun'){draw_chart_of_days(month_details_2013);
      var carName = "Year: 2013 Jun";
      var gameName = "Games: Bioshock Infinite, HD";
      var ratings = "Ratings: 95";
      document.getElementById("demo").innerHTML = (carName+' '+gameName+' '+ratings);}
    else{draw_chart_of_months(months);}
    
   
  }, 1000);
}



function month_selected(month){
  draw_chart_of_months([]);
  setTimeout(function(){
    d3.selectAll(".chart g").remove();
    d3.select(".chart").append("g");
    if(month.Month=='2013'){draw_chart_of_days(month_details_2013);}
    else if(month.Month=='2010'){draw_chart_of_days(month_details_2010);}
    else{draw_chart_of_days(month_details_2008);}
    
    draw_show_months_button();
  }, 1000);
}





function draw_months_back(d){
  draw_chart_of_days([]);
  setTimeout(function(){
    d3.select(".chart").selectAll("*").remove();
    d3.select(".chart").append("g");
    draw_chart_of_months(months);
  }, 1000);
}

function adjustxAxisTextForMonths(selection){
  selection.selectAll("text").attr("transform", "translate(8,0)");
}

function adjustxAxisTextForDays(selection){
  selection.selectAll("text").attr("transform", "translate(4,0)");
}

function generate_random_months_data(){
  for(var i = 0; i < months.length; i++){
    months[i].Quantity = Math.floor(Math.random() * (100 - 0) + 0);
  }
}

function draw_show_months_button(){
  d3.select(".chart").append('text').attr("class", "show_months").attr("x", 130).attr("y", 8).text("Go Back To Years").on("click", function(d){
    draw_months_back();
  });
}

function define_chart_container_width(screen_width){
  if(screen_width > 1000) {
    return 1000;
  } else {
    return screen_width - 30;
  }
}

function define_chart_container_height(screen_height){
  if(screen_height > 500) {
    return 500;
  } else {
    return screen_height - 20;
  }
}

function get_screen_width_height(){
  var w = window,
    d = document,
    e = d.documentElement,
    g = d.getElementsByTagName('body')[0],
    x = w.innerWidth || e.clientWidth || g.clientWidth,
    y = w.innerHeight|| e.clientHeight|| g.clientHeight;
  return {width:x, height:y}
}
