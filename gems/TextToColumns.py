
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from typing import *

class TextToColumns(MacroSpec):
    name: str = "TextToColumns"
    projectName: str = "SnowflakeSqlBasics"
    category: str = "Parse"

    @dataclass(frozen=True)
    class TextToColumnsProperties(MacroProperties):
        # properties for the component with default values
        parameter1: str = "'orders'"
        relation_name: List[str] = field(default_factory=list)

    def dialog(self) -> Dialog:
        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    TextBox("Table Name")
                    .bindPlaceholder("Configure table name")
                    .bindProperty("parameter1")
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diags = []
        print(f"The name of the relation_name is {component.properties.relation_name}")
        diags.append(Diagnostic(f"properties.relation_name", f"relation_name is {component.properties.relation_name}", SeverityLevelEnum.Warning))
        return diags

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the newState's state and return the new state
        all_upstream_nodes = []
        for inputPort in newState.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)
        
        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.slug is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.slug)
        print(f"The name of the relation is {relation_name}") 
        return (replace(newState, properties=replace(newState.properties,relation_name=relation_name)))

    def apply(self, props: TextToColumnsProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"
        non_empty_param = ",".join([param for param in ([props.parameter1] + props.relation_name) if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'      

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return TextToColumns.TextToColumnsProperties(
            parameter1=parametersMap.get('parameter1'),
            relation_name=parametersMap.get('relation_name')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("parameter1", properties.parameter1),
                MacroParameter("relation_name", properties.relation_name)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Validate the component's state
        # updatedComponent = MacroSpec.updateInputPortSlug(component, context)

        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)
        
        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.slug is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.slug)
        print(f"The name of the relation is {relation_name}") 
        return (replace(component, properties=replace(component.properties,relation_name=relation_name)))