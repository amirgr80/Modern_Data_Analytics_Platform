-- health check
SELECT
    toStartOfMinute(order_timestamp) AS minute,
    count() AS orders_per_minute
FROM lakehouse.realtime_orders
WHERE is_valid = 1
  AND order_timestamp >= now() - INTERVAL 6 HOUR
GROUP BY minute
ORDER BY minute;

-- order prices
SELECT
    toStartOfMinute(order_timestamp) AS minute,

    avg(total) AS avg_order_amount,
    min(total) AS min_amount,
    max(total) AS max_amount

FROM lakehouse.realtime_orders
WHERE is_valid=1
  AND order_timestamp>=now()-INTERVAL 6 HOUR
GROUP BY minute
ORDER BY minute;

-- funnel monitoring on sessions
WITH
    clean_events AS
        (
            SELECT
                trimBoth(ifNull(session_id, '')) AS session_id,
                event_type,
                event_timestamp

            FROM lakehouse.realtime_behavioral_events

            WHERE is_valid = 1

              AND length(trimBoth(ifNull(session_id, ''))) > 0

              AND event_timestamp IS NOT NULL
              AND event_timestamp <= now()

              [[AND {{event_date}}]]

              AND event_type IN
                  (
                   'page_view',
                   'product_search',
                   'add_to_cart',
                   'checkout_start',
                   'payment_attempt',
                   'order_complete'
                      )
        ),


    session_funnel AS
        (
            SELECT
                session_id,

                minIf(
                        event_timestamp,
                        event_type = 'page_view'
                ) AS session_start_time,

                countIf(event_type = 'page_view') > 0
                  AS reached_page_view,

                sequenceMatch(
                        '(?1).*(?2)'
                    )(
                        toDateTime(event_timestamp),
                        event_type = 'page_view',
                        event_type = 'product_search'
                ) AS reached_product_search,

                sequenceMatch(
                        '(?1).*(?2).*(?3)'
                    )(
                        toDateTime(event_timestamp),
                        event_type = 'page_view',
                        event_type = 'product_search',
                        event_type = 'add_to_cart'
                ) AS reached_add_to_cart,

                sequenceMatch(
                        '(?1).*(?2).*(?3).*(?4)'
                    )(
                        toDateTime(event_timestamp),
                        event_type = 'page_view',
                        event_type = 'product_search',
                        event_type = 'add_to_cart',
                        event_type = 'checkout_start'
                ) AS reached_checkout_start,

                sequenceMatch(
                        '(?1).*(?2).*(?3).*(?4).*(?5)'
                    )(
                        toDateTime(event_timestamp),
                        event_type = 'page_view',
                        event_type = 'product_search',
                        event_type = 'add_to_cart',
                        event_type = 'checkout_start',
                        event_type = 'payment_attempt'
                ) AS reached_payment_attempt,

                sequenceMatch(
                        '(?1).*(?2).*(?3).*(?4).*(?5).*(?6)'
                    )(
                        toDateTime(event_timestamp),
                        event_type = 'page_view',
                        event_type = 'product_search',
                        event_type = 'add_to_cart',
                        event_type = 'checkout_start',
                        event_type = 'payment_attempt',
                        event_type = 'order_complete'
                ) AS reached_order_complete

            FROM clean_events

            GROUP BY session_id

            HAVING reached_page_view = 1
               AND session_start_time <= now() - INTERVAL 30 MINUTE
        ),

    funnel_counts AS
        (
            SELECT
                countIf(reached_page_view = 1)
                    AS page_view_sessions,

                countIf(reached_product_search = 1)
                    AS product_search_sessions,

                countIf(reached_add_to_cart = 1)
                    AS add_to_cart_sessions,

                countIf(reached_checkout_start = 1)
                    AS checkout_start_sessions,

                countIf(reached_payment_attempt = 1)
                    AS payment_attempt_sessions,

                countIf(reached_order_complete = 1)
                    AS order_complete_sessions

            FROM session_funnel
        ),

    funnel_rows AS
        (
            SELECT
                1 AS stage_order,
                'Page View' AS stage_name,
                page_view_sessions AS sessions,
                page_view_sessions AS previous_stage_sessions,
                page_view_sessions AS starting_sessions
            FROM funnel_counts

            UNION ALL

            SELECT
                2,
                'Product Search',
                product_search_sessions,
                page_view_sessions,
                page_view_sessions
            FROM funnel_counts

            UNION ALL

            SELECT
                3,
                'Add to Cart',
                add_to_cart_sessions,
                product_search_sessions,
                page_view_sessions
            FROM funnel_counts

            UNION ALL

            SELECT
                4,
                'Checkout Start',
                checkout_start_sessions,
                add_to_cart_sessions,
                page_view_sessions
            FROM funnel_counts

            UNION ALL

            SELECT
                5,
                'Payment Attempt',
                payment_attempt_sessions,
                checkout_start_sessions,
                page_view_sessions
            FROM funnel_counts

            UNION ALL

            SELECT
                6,
                'Order Complete',
                order_complete_sessions,
                payment_attempt_sessions,
                page_view_sessions
            FROM funnel_counts
        )

SELECT
    stage_order,
    stage_name,
    sessions,

    round(
            sessions * 100.0
                / nullIf(starting_sessions, 0),
            2
    ) AS conversion_from_start_percent,

    round(
            sessions * 100.0
                / nullIf(previous_stage_sessions, 0),
            2
    ) AS conversion_from_previous_percent,

    if(
            stage_order = 1,
            0,
            previous_stage_sessions - sessions
    ) AS drop_off_sessions,

    if(
            stage_order = 1,
            0,
            round(
                    (
                        previous_stage_sessions - sessions
                        ) * 100.0
                        / nullIf(previous_stage_sessions, 0),
                    2
            )
    ) AS drop_off_rate_percent

FROM funnel_rows

ORDER BY stage_order;

