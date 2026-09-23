{% snapshot snap_sales_persons %}

{{
    config(
        database='silver',
        unique_key='sales_person_id',
        tags=['snapshot'],
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select * from {{ source('bronze', 'sales_persons') }}

{% endsnapshot %}
