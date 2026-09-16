## Summary
<!-- What does this PR do? What problem does it solve? -->

## Type of change
- [ ] Bug fix
- [ ] New feature
- [ ] DQ rule change
- [ ] Config change (dev.yml / prod.yml)
- [ ] Pipeline logic change (bronze / silver / gold)
- [ ] Documentation update

## Changes made
<!-- List the files changed and why -->

## Testing done
- [ ] Unit tests pass locally (`pytest tests/ -v`)
- [ ] Deployed to dev (`databricks bundle deploy --target dev`)
- [ ] Dev pipeline ran successfully
- [ ] Validated counts: Bronze=10, Silver=8, Quarantine=2, Gold=8
- [ ] No prod tables affected

## Validation output
<!-- Paste the pipeline run output here -->
```json

```

## Reviewer checklist
- [ ] Code follows project conventions (no em dashes, bold metric first in comments)
- [ ] DQ rules are correct and intentional
- [ ] Config changes are in the right yml file (dev vs prod)
- [ ] No hardcoded paths or credentials
- [ ] Unit tests cover the change
