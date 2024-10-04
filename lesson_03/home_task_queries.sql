/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
SELECT
    c.name category,
    count(fc.film_id) films_count
FROM
    public.film_category fc
LEFT JOIN public.category c ON
    fc.category_id = c.category_id
GROUP BY
    c.name
ORDER BY
    films_count DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
-- actors by film rentals amount
WITH ar AS (
    SELECT
        count(r.rental_id) film_rentals,
        fa.actor_id
    FROM
        public.rental r
    LEFT JOIN public.inventory i ON
        i.inventory_id = r.inventory_id
    LEFT JOIN public.film_actor fa ON
        i.film_id = fa.film_id
    GROUP BY
        fa.actor_id
    ORDER BY
        film_rentals DESC
    LIMIT 10
)

SELECT
    a.first_name,
    a.last_name,
    ar.film_rentals
FROM
    ar
LEFT JOIN public.actor a ON
    ar.actor_id = a.actor_id
ORDER BY
    ar.film_rentals DESC;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...
WITH top_category AS (
    SELECT
        sum(amount) category_sales,
        fc.category_id
    FROM
        public.payment p
    LEFT JOIN public.rental r ON
        p.rental_id = r.rental_id
    LEFT JOIN public.inventory i ON
        r.inventory_id = i.inventory_id
    LEFT JOIN public.film_category fc ON
        i.film_id = fc.film_id
    GROUP BY
        fc.category_id
    ORDER BY
        category_sales DESC
    LIMIT 1
)

SELECT
    tc.category_sales,
    c.name category
FROM
    top_category tc
LEFT JOIN public.category c ON
    tc.category_id = c.category_id


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...
SELECT
    f.title
FROM
    public.inventory i
RIGHT JOIN public.film f ON
    i.film_id = f.film_id
WHERE
    i.film_id IS NULL


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...
WITH actor_children_films AS (
    SELECT
        fa.actor_id,
        count(fa.actor_id) children_films_amount
    FROM
        public.film f
    LEFT JOIN public.film_category fc ON
        f.film_id = fc.film_id
    LEFT JOIN public.category c ON
        fc.category_id = c.category_id
    LEFT JOIN public.film_actor fa ON
        f.film_id = fa.film_id
    WHERE
        c.name = 'Children'
    GROUP BY
        fa.actor_id
    ORDER BY
        children_films_amount DESC
    LIMIT 3
)

SELECT
    a.first_name,
    a.last_name,
    acf.children_films_amount
FROM
    actor_children_films acf
LEFT JOIN public.actor a ON
    acf.actor_id = a.actor_id

