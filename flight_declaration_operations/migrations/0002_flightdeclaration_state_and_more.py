# Generated by Django 4.0.4 on 2022-07-05 11:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('flight_declaration_operations', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='flightdeclaration',
            name='state',
            field=models.IntegerField(choices=[(1, 'Accepted'), (2, 'Activated'), (3, 'Nonconforming'), (3, 'Contingent'), (4, 'Ended')], default=1, help_text='Set the state of operation'),
        ),
        migrations.AlterField(
            model_name='flightdeclaration',
            name='type_of_operation',
            field=models.IntegerField(choices=[(1, 'VLOS'), (2, 'BVLOS'), (3, 'CREWED')], default=1, help_text='At the moment, only VLOS and BVLOS operations are supported, for other types of operations, please issue a pull-request'),
        ),
    ]
