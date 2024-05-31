
class Variable:

    def __init__(self, name: str, domain: set[object]) -> None:
        self.name = name
        self.domain = domain

    def prune_domain(self, values_to_remove: set[object]) -> None:
        self.domain = self.domain.difference(values_to_remove)
    
    def __repr__(self) -> str:
        return f"Var({self.name})"


def is_assignment_valid(assignment: dict[Variable, object]) -> bool:
    for var in assignment:
        if assignment[var] not in var.domain:
            raise Exception("Invalid assignment.")
    return True


class Constraint:

    def __init__(self, scope: set[Variable], condition) -> None:
        self.scope = scope
        self.condition = condition  # ex. lambda a: a[X] > a[Y]

    def evaluate(self, assignment: dict[Variable, object]) -> bool:
        if not self.is_assignment_correct(assignment):
            return False
        if not is_assignment_valid(assignment):
            return False
        return self.condition(assignment)

    def is_assignment_correct(self, assignment: dict[Variable, object]) -> bool:
        if len(assignment) < len(self.scope):
            raise Exception("Too short assignment.")
        for var in self.scope:
            if assignment.get(var) is None:
                raise Exception("Not full assignment.")
        return True


def get_constraints_from_context(cs: set[Constraint], context: dict[Variable, object]) -> set[Constraint]:
    if not context:
        return set()
    cons_to_evaluate = set()
    for con in cs:
        if con.scope.issubset(context.keys()):
            cons_to_evaluate.add(con)
    return cons_to_evaluate


def evaluate_context(cs: set[Constraint], context: dict[Variable, object]) -> bool:
    for con in cs:
        if not con.evaluate(context):
            return False
    return True


class CSP:

    def __init__(self) -> None:
        self.variables = set()
        self.constraints = set()
        self.solutions = []

    def get_solutions(self):
        return self.solutions

    def add_constraint(self, scope: set[Variable], condition) -> None:
        con = Constraint(scope, condition)
        self.add_constraints(con)
    
    def add_variables(self, *args) -> None:
        self.variables = self.variables.union(set(args))
    
    def add_constraints(self, *args) -> None:
        for con in args:
            if self.is_constraint_valid(con):
                self.constraints.add(con)

    def is_constraint_valid(self, constraint: Constraint) -> bool:
        for var in constraint.scope:
            if var not in self.variables:
                return False
        return True

    def dfs_solve(self) -> None:
        self.solutions = self.dfs_solver(self.variables.copy(), self.constraints.copy(), dict())

    def dfs_solver(self, vs: set[Variable], cs: set[Constraint],
                   context: dict[Variable, object]) -> list[dict[Variable, object]]:
        context_cons = get_constraints_from_context(cs, context)
        if not evaluate_context(context_cons, context):
            return []
        if not vs:
            return [context]
        new_vs = vs.copy()
        var = new_vs.pop()
        new_cs = cs.copy().difference(context_cons)
        sols = []
        for val in var.domain:
            new_context = context.copy()
            new_context[var] = val
            sols.extend(self.dfs_solver(new_vs, new_cs, new_context))
        return sols
