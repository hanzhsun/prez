from typing import Optional, List, Union, Tuple

from pydantic import BaseModel
from rdflib import RDF, URIRef

from prez.reference_data.prez_ns import PREZ
from temp.grammar import (
    Var,
    LANGTAG,
    BooleanLiteral,
    PrimaryExpression,
    GroupGraphPattern,
    GroupGraphPatternSub,
    SimplifiedTriple,
    Bind,
    Expression,
    GraphPatternNotTriples,
    NumericLiteral,
    BuiltInCall,
    Filter,
    RDFLiteral,
    RegexExpression,
    Constraint,
    GroupOrUnionGraphPattern,
    OptionalGraphPattern,
    FunctionCall,
    ArgList,
    BrackettedExpression,
    InlineData,
    InlineDataOneVar,
    DataBlock,
    IRI,
    SelectClause,
    Aggregate,
    SubSelect,
    GroupClause,
    GroupCondition,
    SolutionModifier,
    WhereClause,
    OrderClause,
    OrderCondition,
    SubSelectString,
    ConstructTemplate,
    ConstructTriples,
    ConstructQuery,
    LimitClause,
    LimitOffsetClauses,
    OffsetClause,
    DataBlockValue,
)


class SearchQuery(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    search_term: str
    pred_vals: List[URIRef]
    additional_ss: Optional[SubSelect] = None
    limit: int = 10
    offset: int = 0

    sr_uri: Var = Var(value="focus_node")
    pred: Var = Var(value="pred")
    match: Var = Var(value="match")
    weight: Var = Var(value="weight")
    hashid: Var = Var(value="hashID")
    w: Var = Var(value="w")

    @property
    def sr_uri_pe(self):
        return PrimaryExpression(content=self.sr_uri)

    @property
    def pred_pe(self):
        return PrimaryExpression(content=self.pred)

    @property
    def match_pe(self):
        return PrimaryExpression(content=self.match)

    @property
    def weight_pe(self):
        return PrimaryExpression(content=self.weight)

    @property
    def w_pe(self):
        return PrimaryExpression(content=self.w)

    @property
    def inner_select_vars(self):
        return {
            "one": {
                "weight_val": 100,
                "function": "LCASE",
                "prefix": "",
                "case_insensitive": None,
            },
            "two": {
                "weight_val": 20,
                "function": "REGEX",
                "prefix": "^",
                "case_insensitive": True,
            },
            "three": {
                "weight_val": 10,
                "function": "REGEX",
                "prefix": "",
                "case_insensitive": True,
            },
        }

    def render(self):
        cq = self.create_construct_query()
        return "".join(part for part in cq.render())

    def create_construct_query(self):
        cq = ConstructQuery(
            construct_template=self.create_construct_template(),
            where_clause=WhereClause(
                group_graph_pattern=GroupGraphPattern(
                    content=self.create_outer_subselect()
                )
            ),
            solution_modifier=SolutionModifier(),
        )
        return cq

    def create_construct_template(self):
        """
        ?hashID a prez:SearchResult ;
        prez:searchResultWeight ?weight ;
        prez:searchResultPredicate ?predicate ;
        prez:searchResultMatch ?match ;
        prez:searchResultURI ?search_result_uri .
        """
        search_result_triples = [
            SimplifiedTriple(
                subject=self.hashid,
                predicate=IRI(value=PREZ.searchResultWeight),
                object=self.weight,
            ),
            SimplifiedTriple(
                subject=self.hashid,
                predicate=IRI(value=PREZ.searchResultPredicate),
                object=self.pred,
            ),
            SimplifiedTriple(
                subject=self.hashid,
                predicate=IRI(value=PREZ.searchResultMatch),
                object=self.match,
            ),
            SimplifiedTriple(
                subject=self.hashid,
                predicate=IRI(value=PREZ.searchResultURI),
                object=self.sr_uri,
            ),
            SimplifiedTriple(
                subject=self.hashid,
                predicate=IRI(value=RDF.type),
                object=IRI(value=PREZ.SearchResult),
            ),
        ]
        ct = ConstructTemplate(
            construct_triples=ConstructTriples(triples=search_result_triples)
        )
        return ct

    def create_outer_subselect(self):
        outer_ss = SubSelect(
            select_clause=self.create_outer_select_clause(),
            where_clause=self.create_outer_where_clause(),
            solution_modifier=self.create_solution_modifier(),
        )
        return outer_ss

    def create_outer_select_clause(self):
        """
        SELECT ?focus_node ?predicate ?match ?weight (URI(CONCAT("urn:hash:", SHA256(CONCAT(STR(?focus_node), STR(?predicate), STR(?match), STR(?weight))))) AS ?hashID)
        """
        expressions = [self.sr_uri_pe, self.pred_pe, self.match_pe, self.weight_pe]
        str_builtins = [BuiltInCall.create_with_one_expr("STR", e) for e in expressions]
        str_expressions = [PrimaryExpression(content=b) for b in str_builtins]
        inner_concat = BuiltInCall.create_with_n_expr("CONCAT", str_expressions)
        sha256_expr = PrimaryExpression(
            content=BuiltInCall.create_with_one_expr(
                "SHA256", PrimaryExpression(content=inner_concat)
            )
        )
        urn_literal = PrimaryExpression(content=RDFLiteral(value="urn:hash:"))
        outer_concat = BuiltInCall.create_with_n_expr(
            "CONCAT", [urn_literal, sha256_expr]
        )
        uri_expr = BuiltInCall.create_with_one_expr(
            "URI", PrimaryExpression(content=outer_concat)
        )
        uri_pr_exp = PrimaryExpression(content=uri_expr)
        uri_exp = Expression.from_primary_expr(uri_pr_exp)
        sc = SelectClause(
            variables_or_all=[
                self.sr_uri,
                self.pred,
                self.match,
                self.weight,
                (uri_exp, self.hashid),
            ]
        )
        return sc

    def create_outer_where_clause(self):
        """Wrapper WHERE clause"""
        inner_ss = self.create_inner_subselect()
        inner_ggp = GroupGraphPattern(content=inner_ss)
        outer_wc = WhereClause(group_graph_pattern=inner_ggp)
        return outer_wc

    def create_solution_modifier(self):
        """ORDER BY DESC(?weight)"""
        ocond = OrderCondition(var=self.weight, direction="DESC")
        oclause = OrderClause(conditions=[ocond])
        limit = LimitClause(limit=self.limit)
        offset = OffsetClause(offset=self.offset)
        loc = LimitOffsetClauses(limit_clause=limit, offset_clause=offset)
        sm = SolutionModifier(order_by=oclause, limit_offset=loc)
        return sm

    def create_inner_subselect(self):
        inner_ss = SubSelect(
            select_clause=self.create_inner_select_clause(),
            where_clause=self.create_inner_where_clause(),
            solution_modifier=self.create_group_by_solution_modifier(),
        )
        return inner_ss

    def create_group_by_solution_modifier(self):
        """
        GROUP BY ?focus_node ?predicate ?match
        """
        gc_sr_uri = GroupCondition(condition=self.sr_uri)
        gc_pred = GroupCondition(condition=self.pred)
        gc_match = GroupCondition(condition=self.match)
        gc = GroupClause(group_conditions=[gc_sr_uri, gc_pred, gc_match])
        sm = SolutionModifier(group_by=gc)
        return sm

    def create_inner_select_clause(self):
        """
        SELECT ?focus_node ?predicate ?match (SUM(?w) AS ?weight)
        """
        pr_exp = PrimaryExpression(content=self.w)
        exp = Expression.from_primary_expr(pr_exp)
        sum_agg = Aggregate(function_name="SUM", expression=exp)
        sum_bic = BuiltInCall(other_expressions=sum_agg)
        sum_pr_exp = PrimaryExpression(content=sum_bic)
        sum_exp = Expression.from_primary_expr(sum_pr_exp)
        sc = SelectClause(
            variables_or_all=[
                self.sr_uri,
                self.pred,
                self.match,
                (sum_exp, self.weight),
            ]
        )
        return sc

    def create_inner_where_clause(self):
        # outer group graph pattern sub
        iri_pred_vals = [IRI(value=p) for p in self.pred_vals]
        iri_db_vals = [DataBlockValue(value=p) for p in iri_pred_vals]
        ildov = InlineDataOneVar(variable=self.pred, datablockvalues=iri_db_vals)
        ild = InlineData(data_block=DataBlock(block=ildov))
        gpnt_ild = GraphPatternNotTriples(content=ild)

        # union statements
        gougp = self.create_union_of_inner_ggps()
        gpnt_gougp = GraphPatternNotTriples(content=gougp)

        outer_ggps = GroupGraphPatternSub(
            graph_patterns_or_triples_blocks=[gpnt_ild, gpnt_gougp]
        )
        outer_ggp = GroupGraphPattern(content=outer_ggps)
        wc = WhereClause(group_graph_pattern=outer_ggp)
        return wc

    def create_union_of_inner_ggps(self):
        # inner group graph patterns (unioned statements)
        inner_select_ggp_list = []
        for var_dict in self.inner_select_vars.values():
            inner_select_ggp_list.append(self.create_inner_ggp(**var_dict))
        gougp = GroupOrUnionGraphPattern(group_graph_patterns=inner_select_ggp_list)
        return gougp

    def create_inner_ggp(
        self,
        weight_val: int,
        function: str,
        prefix: str,
        case_insensitive: Optional[bool],
    ) -> GroupGraphPattern:
        ggp = GroupGraphPattern(content=GroupGraphPatternSub())

        # triple pattern  e.g. (?focus_node ?pred ?match)
        ggp.content.add_triple(
            SimplifiedTriple(
                subject=self.sr_uri,
                predicate=self.pred,
                object=self.match,
            )
        )

        # add additional focus node selection e.g. from endpoint definitions
        if self.additional_ss:
            if isinstance(self.additional_ss, SubSelectString):
                ss_ggp = GroupGraphPattern(content=self.additional_ss)
                gougp = GroupOrUnionGraphPattern(group_graph_patterns=[ss_ggp])
                gpnt = GraphPatternNotTriples(content=gougp)
                ggp.content.add_pattern(gpnt)
            elif isinstance(self.additional_ss, SubSelect):
                ss_ggps = self.additional_ss.where_clause.group_graph_pattern.content
                ss_tb = ss_ggps.triples_block
                ss_gpotb = ss_ggps.graph_patterns_or_triples_blocks
                if ss_tb:
                    ggp.content.add_pattern(ss_tb)
                if ss_gpotb:
                    for pattern in ss_gpotb:
                        ggp.content.add_pattern(pattern)

        # bind  e.g. BIND(100 AS ?w)
        bind_for_w = Bind(
            expression=Expression.from_primary_expr(
                PrimaryExpression(content=NumericLiteral(value=weight_val))
            ),
            var=Var(value="w"),
        )
        bind_gpnt = GraphPatternNotTriples(content=bind_for_w)
        ggp.content.add_pattern(bind_gpnt)

        # FILTER (REGEX(?match, "^$term", "i"))
        pe_st = PrimaryExpression(content=RDFLiteral(value=(prefix + self.search_term)))
        if function == "REGEX":
            e_ci = None
            if case_insensitive:
                pe_ci = PrimaryExpression(content=RDFLiteral(value="i"))
                e_ci = Expression.from_primary_expr(pe_ci)
            regex_expression = RegexExpression(
                text_expression=Expression.from_primary_expr(
                    self.match_pe
                ),  # Expression for the text
                pattern_expression=Expression.from_primary_expr(pe_st),  # Search Term
                flags_expression=e_ci,  # Case insensitivity
            )
            bic = BuiltInCall(other_expressions=regex_expression)
            cons = Constraint(content=bic)
            filter_expr = Filter(constraint=cons)
        # filter e.g. FILTER(LCASE(?match) = "search term")
        elif function == "LCASE":
            bifc = BuiltInCall(function_name=function, arguments=[self.match])
            pe_focus = PrimaryExpression(content=bifc)
            filter_expr = Filter.filter_relational(
                focus=pe_focus, comparators=pe_st, operator="="
            )
        else:
            raise ValueError("Only LCASE and REGEX handled at present")
        filter_gpnt = GraphPatternNotTriples(content=filter_expr)
        ggp.content.add_pattern(filter_gpnt)
        return ggp


# if __name__ == "__main__":
#     # additional_ss = SubSelectString(select_string="SELECT * {?focus_node a owl:Class}")
#     sr_uri = Var(value="focus_node")
#     additional_ss = SubSelect(
#         select_clause=SelectClause(variables_or_all=[sr_uri]),
#         where_clause=WhereClause(
#             group_graph_pattern=GroupGraphPattern(
#                 content=GroupGraphPatternSub(
#                     triples_block=TriplesBlock(
#                         triples=[
#                             SimplifiedTriple(
#                                 subject=sr_uri,
#                                 predicate=IRI(
#                                     value="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
#                                 ),
#                                 object=IRI(value="http://www.w3.org/2002/07/owl#Class"),
#                             )
#                         ]
#                     )
#                 )
#             )
#         ),
#     )
#     sq = SearchQuery(
#         search_term="test",
#         pred_vals=[RDFS.label],
#         additional_ss=additional_ss,
#     ).render()
#     print(sq)
