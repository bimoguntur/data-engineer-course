WITH films_with_ratings_actors AS (
    SELECT
        f.film_id,
        f.title,
        f.release_date,
        f.price,
        f.rating,
        f.user_rating,
        CASE 
            WHEN f.user_rating >= 4.5 THEN 'EXCELLENT'
            WHEN f.user_rating >= 4.0 THEN 'GOOD'
            WHEN f.user_rating >= 3.0 THEN 'AVERAGE'
            ELSE 'POOR'
        END AS rating_category,
        STRING_AGG(a.actor_name, ' , ') actors
    FROM {{ ref('films') }} f
    LEFT JOIN {{ ref('film_actors' )}} fa ON f.film_id = fa.film_id
    LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY 1,2,3,4,5,6
)
SELECT *
FROM films_with_ratings_actors