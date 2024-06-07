count_requests_per_subborough(subborough(S), Count) :-
findall(Request, (incident_subborough(request(Request), subborough(S))), Requests),
length(Requests, Count).
average_requests_per_subborough(Average) :-
findall(Count, (subborough(S), count_requests_per_subborough(S, Count)), Counts),
sum_list(Counts, Total),
length(Counts, NumberOfSubboroughs),
Average is Total / NumberOfSubboroughs.
date_to_julian(date(D, M, Y), Julian) :-
Julian is D + M * 30 + Y * 365.  % Semplificazione per il calcolo dei giorni.
days_between(date(D1, M1, Y1), date(D2, M2, Y2), Days) :-
date_to_julian(date(D1, M1, Y1), Julian1),
date_to_julian(date(D2, M2, Y2), Julian2),
Days is Julian2 - Julian1.
is_closed(request(R)) :-
status(request(R), 'closed').
days_to_close(request(R), Days) :-
created_date(request(R), CreatedDate),
closed_date(request(R), ClosedDate),
ClosedDate \= unknown,
days_between(CreatedDate, ClosedDate, Days).
is_closed_immediately(request(R)) :-
is_closed(request(R)),
days_to_close(request(R), Days),
Days =:= 0.
majority_white(subborough(S)) :-
white_pop(subborough(S), Wh),
Wh > 60.
minority_white(subborough(S)) :-
white_pop(subborough(S), Wh),
Wh < 20.
racial_diversity_index(subborough(S), Index) :-
white_pop(subborough(S), Wh),
black_pop(subborough(S), Bl),
asian_pop(subborough(S), As),
hispanic_pop(subborough(S), Hs),
IdealPercentage is 25.0,
DeviationWh is (Wh - IdealPercentage) ** 2,
DeviationBl is (Bl - IdealPercentage) ** 2,
DeviationAs is (As - IdealPercentage) ** 2,
DeviationHs is (Hs - IdealPercentage) ** 2,
SumOfDeviations is DeviationWh + DeviationBl + DeviationAs + DeviationHs,
Index is 100 - sqrt(SumOfDeviations / 4).
