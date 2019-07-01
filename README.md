# Games'ON 

For gamers, by gamers. [Link](https://docs.google.com/presentation/d/1UiaVdh7vm3-zZy_zAdr0yUroDv7sSAVyL2oXPm6rg1g/edit#slide=id.p)


<hr/>

## Introduction
Developing new games are becoming more and more expensive.

Developers can learn by gamers:  MOD for games, discussion platforms like
Reddit and Steam where developers can learn from gamers about their opinions and ideas.

For gamers mean that developers can also satisfy gamers want, make remaster version of games which can satisfy both sales volume and gamers anticipation.


## Data
Reddit comments data from [Pushshift.io](https://files.pushshift.io/reddit/).
In this project, I use the data from 2017 to 2015, around 700GB of uncompressed JSON data in total. Defined schema after preprocessing is listed as:

Key | Value Type
----| ----------
created_utc | int (utc)
id | str
score | int
author | str
body | str
subreddit | str
name | str

Amazon Customer review data from [Amazon S3](https://s3.amazonaws.com/amazon-reviews-pds/readme.html). The Amazon customer review data varies from 1995 to 2015, has around 170GB of data. In this project, I only query under the video games subcategory.

Steam selling ranking data from [steam250](https://steam250.com/). This platform offers year by year best selling games with ratings.

## Architecture
<img src="./img/architecture.png" width="800px"/>

## Engineering challenges

