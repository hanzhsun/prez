from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel
from rdflib import URIRef, BNode, Graph
from rdflib.namespace import SH, RDF

from temp.grammar import IRI, SimplifiedTriple, TriplesBlock, Var, SelectClause, GraphPatternNotTriples, InlineData, \
    DataBlock, InlineDataOneVar, DataBlockValue


class SHACL(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    def from_graph(self, graph):
        raise NotImplementedError("Subclasses must implement this method.")

    def to_grammar(self):
        raise NotImplementedError("Subclasses must implement this method.")


class NodeShape(SHACL):
    uri: URIRef
    nodeTarget: Optional[URIRef]
    classTarget: Optional[List[URIRef]]
    subjectsOfTarget: Optional[URIRef]
    objectsOfTarget: Optional[URIRef]
    propertyShapes: Optional[List[URIRef]]
    _triples: Optional[List[SimplifiedTriple]]

    def from_shacl_graph(self, graph):  # TODO this can be a SPARQL select against the system graph.
        self.nodeTarget = next(graph.objects(self.uri, SH.targetNode), None)
        self.classTarget = list(graph.objects(self.uri, SH.targetClass))
        self.subjectsOfTarget = next(graph.objects(self.uri, SH.targetSubjectsOf), None)
        self.objectsOfTarget = next(graph.objects(self.uri, SH.targetObjectsOf), None)
        self.propertyShapes = list(graph.objects(self.uri, SH.property))

    def to_listing_select(self) -> TriplesBlock:
        focus_node = Var(value="focus_node")
        if self.nodeTarget:
            pass  # do not need to add any specific triples or the like
        if self.classTarget:
            self._process_class_target(focus_node)
        if self.subjectsOfTarget:
            pass
        if self.objectsOfTarget:
            pass
        if self.propertyShapes:
            self._process_property_shapes()

    def to_link_select(self, focus_node) -> SelectClause:

    def _process_class_target(self, focus_node):
        for klass in self.classTarget:
            self._triples.append(
                SimplifiedTriple(
                    subject=focus_node,
                    predicate=IRI(value=RDF.type),
                    object=klass,
                )
            )

    def _process_subjects_of_target(self):
        # ?focus_node pred ?obj - ?obj is constrained by e.g. sh:class in a property shape.
        self._triples.append(
            SimplifiedTriple(
                subject=self.focus_node,
                predicate=IRI(value=self.subjectsOfTarget),
                object=Var(value="ValidationNode"),
            )
        )

    def _process_objects_of_target(self):
        self._triples.append(
            SimplifiedTriple(
                subject=Var(value="ValidationNode"),
                predicate=IRI(value=self.objectsOfTarget),
                object=self.focus_node,
            )
        )

    def _process_property_shapes(self):
        for shape in self.propertyShapes:
            ps = PropertyShape(shape)
            self._triples.append(ps.to_grammar)


class PropertyShape(SHACL):
    uri: URIRef  # URI of the shape
    graph: Graph  # the graph containing the property shape
    # inputs
    property_paths: Optional[List[Union[URIRef, BNode]]]
    or_klasses: List[URIRef]
    # outputs
    _st_list = Optional[List[SimplifiedTriple]]
    _gpnt_list = Optional[List[GraphPatternNotTriples]]
    _select_vars: Optional[List[Var]]

    def from_graph(self, graph):
        _single_class = next(graph.objects(self.uri, SH["class"]), None)
        if _single_class:
            klasses = list(_single_class)
        else:
            # _multiple_classes = list(graph.objects(self.uri, SH["class"]), None)
            klasses = _single_class # if _single_class else _multiple_classes
            pass
        # TODO logic for or statement
        self.property_paths = list(graph.objects(self.uri, SH.path))

    def to_grammar(self, focus_node: Union[Var, IRI]):
        # focus node = URI when generating links; Variable when listing objects
        # process class statements NB this is the class on validation nodes
        # get the length of any property path chains; this is what the target class applies to.
        for pp in self.property_paths:
            if isinstance(pp, BNode):
                pred_objects_gen = self.profile_graph.predicate_objects(
                    subject=pp
                )
                bn_pred, bn_obj = next(pred_objects_gen, (None, None))
                if bn_obj == SH.union:
                    pass
                elif bn_pred == SH.inversePath:
                    inverse_preds.append(IRI(value=bn_obj))
                elif bn_pred == SH.alternativePath:
                    predicates.extend(list(Collection(self.profile_graph, bn_obj)))
                else:  # sequence paths
                    predicates.append(tuple(Collection(self.profile_graph, path_obj)))
            else:  # a plain path specification to restrict the predicate to a specific value
                predicates.append(path_obj)
        else:  # a plain path specification to restrict the predicate to a specific value
            predicates.append(path_obj)


        if self.property_paths:
            for property_path in self.property_paths:
                if isinstance(property_path, URIRef):
                    # vanilla property path
                    self._st_list.append(
                        SimplifiedTriple(
                            subject=focus_node,
                            predicate=IRI(value=property_path),
                            object=Var(value="ValidationNode")
                        )
                    )
                elif isinstance(property_path, BNode):
                    pred_objects_gen = self.profile_graph.predicate_objects(
                        subject=path_obj
                    )
                    bn_pred, bn_obj = next(pred_objects_gen, (None, None))
                    if bn_obj == SH.union:
                        pass
                    elif bn_pred == SH.inversePath:
                        inverse_preds.append(IRI(value=bn_obj))
                    elif bn_pred == SH.alternativePath:
                        predicates.extend(list(Collection(self.profile_graph, bn_obj)))
                    else:  # sequence paths
                        predicates.append(tuple(Collection(self.profile_graph, path_obj)))
                else:  # a plain path specification to restrict the predicate to a specific value
                    predicates.append(path_obj)

        if self.or_klasses:
            if len(self.or_klasses) == 1:
                self._st_list.append(
                        SimplifiedTriple(
                            subject=Var(value="ValidationNode"),
                            predicate=IRI(value=RDF.type),
                            object=IRI(value=self.or_klasses[0])
                        )
                )
            else:
                self._st_list.append(
                    SimplifiedTriple(value="ValidationNode"),
                    IRI(value=RDF.type),
                    Var(value="ValClasses")
                )
                dbvs = [DataBlockValue(value=IRI(value=klass)) for klass in self.or_klasses]
                self._gpnt_list.append(
                    GraphPatternNotTriples(
                        content=InlineData(
                            data_block=DataBlock(
                                block=InlineDataOneVar(
                                    variable=Var(value="ValClasses"),
                                    datablockvalues=dbvs
                                )
                            )
                        )
                    )
                )




class PropertyPath(SHACL):
    uri: URIRef

class Path(SHACL):
    focus_uri: Union[IRI, Var]
    path_uri: URIRef

    def to_grammar(self):
        return SimplifiedTriple(self.focus_uri, IRI(value=self.uri), Var(value="ValidationNode"))

class InversePath(SHACL):
    focus_uri: Union[IRI, Var]
    inverse_uri: URIRef