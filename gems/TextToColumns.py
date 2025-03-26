
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

class TextToColumns(MacroSpec):
    name: str = "TextToColumns"
    projectName: str = "SnowflakeSqlBasics"
    category: str = "Parse"

    @dataclass(frozen=True)
    class TextToColumnsProperties(MacroProperties):
        # properties for the component with default values
        parameter1: str = "'orders'"
        relation_name: str = ""

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
        return super().validate(context, component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = newState.ports.inputs[0].slug
        return (replace(newState, properties=replace(newState.properties,relation_name=relation_name)))

    def apply(self, props: TextToColumnsProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"
        non_empty_param = ",".join([param for param in [props.parameter1,props.relation_name]])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'      

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return TextToColumns.TextToColumnsProperties(
            parameter1=parametersMap.get('parameter1'),
            relation_name="in0"
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("parameter1", properties.parameter1)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Validate the component's state
        updatedComponent = MacroSpec.updateInputPortSlug(component, context)
        relation_name = updatedComponent.ports.inputs[0].slug

        # Save the relation_name in the instance variable for later use
        print(f"The name of the relation is {relation_name}") 
        return (replace(component, properties=replace(component.properties,relation_name=relation_name)))