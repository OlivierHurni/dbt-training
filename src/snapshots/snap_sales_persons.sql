{% snapshot snap_sales_persons %}

{{
    config(
        database='silver',
        schema='sports_shop',
        unique_key='sales_person_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select * from {{ source('bronze', 'sales_persons') }}

{% endsnapshot %}
