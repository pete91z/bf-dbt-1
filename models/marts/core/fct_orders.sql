with payments as (
    select * from {{ref('stg_payments')}}
    where status='success'
),

orders as (
    select * from {{ref('stg_orders')}}
),

customers as (
    select * from {{ref('stg_customers')}}
),

final_fct as (
    select orders.customer_id,
           orders.order_id,
           coalesce(payments.amount,0) as amount
    from orders
    left join payments on (orders.order_id=payments.payment_id)
)

select * from final_fct