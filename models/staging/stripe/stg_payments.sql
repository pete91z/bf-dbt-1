with payments as (
    select id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    amount,
    created as created_date
    from stripe.payment
)

select * from payments