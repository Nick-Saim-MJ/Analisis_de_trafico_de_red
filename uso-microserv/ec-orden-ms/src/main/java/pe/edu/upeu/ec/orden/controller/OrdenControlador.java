package pe.edu.upeu.ec.orden.controller;

import pe.edu.upeu.ec.orden.entity.Orden;
import pe.edu.upeu.ec.orden.service.OrdenServicio;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ordenes")
@RequiredArgsConstructor
public class OrdenControlador {

    private final OrdenServicio ordenServicio;

    
    @GetMapping
    public List<Orden> listarOrdenes() {
        return ordenServicio.listarOrdenes();
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Orden crearOrden(@RequestBody Orden orden) {
        return ordenServicio.crearOrden(orden);
    }
}
