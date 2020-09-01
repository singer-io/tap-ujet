# Changelog

## 1.0.0
  * Releasing

## 0.0.4
  * Fix [ISSUE #5: Errors During Transform](https://github.com/singer-io/tap-ujet/issues/5); JSON schema null handling.

## 0.0.3
  * Fix for incremental bookmark query param

## 0.0.2
  * Update schemas: agent_activity_logs, agents and calls
    * Add nulls for types
    * Add date-times where missing
  * Remove unused date_window_size from config
  * Fix bookmark config for calls stream
  * Fix pagination logging in sync
  * Update documentation

## 0.0.1
  * Initial commit
