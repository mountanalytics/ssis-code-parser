import xmltodict


class Load():
    def __init__(self, path):
        self.path = path

    def remove_at_signs(self, obj):
        if isinstance(obj, dict):
            return {key.replace('@', ''): self.remove_at_signs(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.remove_at_signs(item) for item in obj]
        else:
            return obj
        
    def remove_first_layer(self, json_dict):
        # Extract values from the first layer
        values = list(json_dict.values())
        return values

    def run(self):

        # Path to the XML file
        with open(self.path, 'rb') as f:
            self.xml = f.read()
   
        # open the xml file
        o = xmltodict.parse(self.xml)  # every time you reload the file in colab the key changes (file (1).xml becomes file (2).xml ...)

        json = self.remove_first_layer(self.remove_at_signs(o))[0]
        return json