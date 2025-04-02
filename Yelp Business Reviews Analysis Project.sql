-- creating the table for loading all Yelp reviews
create or replace table yelp_reviews (review_text variant);

-- creating the table for loading all Yelp business data but json
create or replace table yelp_businesses (business_text variant);

-- Cross-checking the data in JSON format
select * from yelp_businesses limit 100;
select * from yelp_reviews limit 10;

-- creating a custom function for sentiment analysis using Python Package
create or replace function analyze_sentiment(text string)
returns string
language python
runtime_version = '3.8'
packages = ('textblob')
handler = 'sentiment_analyzer'
as $$
from textblob import TextBlob
def sentiment_analyzer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;

-- converting data from JSON format to tabular format - for the yelp_reviews table
create table tbl_yelp_reviews as 
select 
    review_text:business_id::string as business_id, 
    review_text:date::date as review_date,
    review_text:user_id::string as user_id,
    review_text:stars::number as review_stars,
    review_text:text::string as review_text,
    analyze_sentiment(review_text) as sentiments
from yelp_reviews;

select count(*) from tbl_yelp_reviews;

-- converting data from JSON format to tabular format - for the yelp_businesses table
create or replace table tbl_yelp_businesses as 
select
    business_text:business_id::string as business_id,
    business_text:name::string as name,
    business_text:city::string as city,
    business_text:state::string as state,
    business_text:review_count::integer as review_count,
    business_text:stars::number as stars,
    business_text:categories::string as categories
from yelp_businesses;

select * from tbl_yelp_businesses;
select * from tbl_yelp_reviews limit 100;

-- Solving all the 10 Questions to generate Business Insights 

-- Question 1: Find the number of businesses in each category
with cte as (
select
    business_id,
    trim(A.value) as category
from tbl_yelp_businesses, 
lateral split_to_table(categories,',') A
) 
select
    category,
    count(category) as no_of_businesses
from cte
group by category
order by no_of_businesses desc;


-- Question 2: Find top 10 users who have reviewed the most businesses in the "Restaurants" Category. 
select 
    r.user_id as users,
    count(distinct(r.business_id)) as count_of_reviews
from tbl_yelp_reviews r
join tbl_yelp_businesses b
on r.business_id = b.business_id
where b.categories ilike '%restaurant%'
group by users
order by count_of_reviews desc
limit 10;


-- Question 3: Find the most popular categories of businesses (based on the number of reviews)
with cte as (
select
    r.business_id,
    trim(A.value) as category,
    r.review_text
from tbl_yelp_businesses b
join tbl_yelp_reviews r
on b.business_id = r.business_id,
lateral split_to_table(b.categories, ',') A
)
select
    category,
    count(review_text) as no_of_reviews
from cte
group by category
order by no_of_reviews desc;


-- Question 4: Find the top 3 most recent reviews for each business
with cte as (
select
    r.business_id,
    b.name,
    review_date,
    review_text,
    row_number() over(partition by r.business_id order by review_date desc) as latest_review
from tbl_yelp_reviews r
join tbl_yelp_businesses b
on r.business_id = b.business_id
) 
select 
    business_id,
    name,
    review_date,
    review_text
from cte
where latest_review <= 3;


-- Question 5: Find the month with the highest number of reviews
select 
    month(review_date) as month,
    count(review_text) as no_of_reviews
from tbl_yelp_reviews
group by month
order by no_of_reviews desc
limit 1;


-- Question 6: Find the percentage of 5 star reviews for each business
select
    r.business_id,
    b.name,
    count(*) as total_reviews,
    sum(
        case
            when r.review_stars = 5 then 1
            else 0
        end) as five_star_reviews,
    round(five_star_reviews / total_reviews * 100, 2) as five_star_rev_perc
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b
on r.business_id = b.business_id
group by r.business_id, b.name;


-- Question 7: Find the top 5 most reviewed businesses in each city
with cte as (
select
    r.business_id,
    b.name,
    b.city,
    count(review_text) as count_of_reviews,
    dense_rank() over(partition by b.city order by count(review_text) desc) as rnk
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b
on r.business_id = b.business_id
group by r.business_id, b.name, b.city
) 
select  
    business_id,
    name, 
    city,
    count_of_reviews
from cte
where rnk <=5;


-- Question 8 - Find the average rating of businesses that have at least 100 reviews
select
    r.business_id,
    b.name,
    count(*) as total_reviews,
    round(avg(r.review_stars),1) as avg_rating
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b
on r.business_id = b.business_id
group by r.business_id, b.name
having total_reviews >= 100;


-- Question 9 - List the top 10 users who have written the most reviews, along with the businesses they reviewed
with cte as (
select
    user_id, 
    count(*) as total_reviews
from tbl_yelp_reviews
group by user_id
order by total_reviews desc
limit 10
) 
select
    cte.user_id,
    r.business_id, 
    b.name
from cte
join tbl_yelp_reviews r
on cte.user_id = r.user_id
join tbl_yelp_businesses b
on r.business_id = b.business_id
group by cte.user_id, r.business_id, b.name
order by cte.user_id;


-- Question 10: Find top 10 businesses with highest positive sentiment reviews 
select
    r.business_id,
    b.name,
    count(*) as total_reviews
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b
on r.business_id = b.business_id
where r.sentiments = 'Positive'
group by r.business_id, b.name
order by total_reviews desc
limit 10;