"""Process role runners loaded lazily by ``queue_service.cli``.

Individual role modules (``api``, ``migrate``, ``maintain``, ``relay``,
``apply``) are imported only after the CLI validates the role name. Keep this
package init minimal so concurrent role plans can add modules without conflicts.
"""
