.. _intro:

=============
Introduction
=============

FireX Kit provides:

  * A ``FireXTask`` class derived from Celery's base `Task`_ class that facilitates passing of arguments and return values from one task to another, which is particularly useful when using `Chains`_.
  * A mechanism to elegantly and dynamically convert task arguments.
  * Helper functions that faciliate checking for readiness of AsyncResult objects.

.. _`Task`: http://docs.celeryproject.org/en/latest/userguide/tasks.html
.. _`Chains`: http://docs.celeryproject.org/en/latest/userguide/canvas.html#chains

