{% docs l_returnflag %}
	
One of the following values: 

| returnflag     | definition                    |
|----------------|-------------------------------|
| R              | The product has been returned |
| A              | The product has been accepted |
| N              | No information                | 

{% enddocs %}

{% docs l_linestatus %}
	
One of the following values: 

| linestatus | definition                                                                                                     |
|------------|----------------------------------------------------------------------------------------------------------------|
| F          | Fulfilled - the line item has been successfully processed, and the products have been shipped to the customer. |
| O          | Open - the line item is still pending and has not been fully processed or shipped.                             |

{% enddocs %}

{% docs o_orderstatus %}
	
One of the following values: 

| linestatus | definition                                                                                                |
|------------|-----------------------------------------------------------------------------------------------------------|
| F          | Fulfilled - all lineitems of this order have L_LINESTATUS set to "F". |
| O          | Open - all lineitems of this order have L_LINESTATUS set to "O".   |
| P          | Pending -  otherwise.   |

{% enddocs %}